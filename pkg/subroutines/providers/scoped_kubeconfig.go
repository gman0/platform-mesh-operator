/*
Copyright 2025.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package providers

import (
	"context"

	gcerrors "github.com/platform-mesh/golang-commons/errors"
	"github.com/platform-mesh/golang-commons/logger"
	"github.com/platform-mesh/subroutines"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	kerrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/clientcmd"
	clientcmdapi "k8s.io/client-go/tools/clientcmd/api"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	providersv1alpha1 "github.com/platform-mesh/platform-mesh-operator/api/providers/v1alpha1"
)

const (
	ScopedKubeconfigSubroutineName = "ScopedKubeconfigSubroutine"
	scopedKubeconfigFinalizer      = "providers.platform-mesh.io/scoped-kubeconfig-finalizer"

	providerSANamespace = "default"
)

func providerKubeconfigSecretName(provider *providersv1alpha1.Provider) string {
	return "platform-mesh-provider-token-" + provider.Name
}

func providerServiceAccountName(provider *providersv1alpha1.Provider) string {
	return "platform-mesh-provider-" + provider.Name
}

func providerServiceAccountTokenSecretName(provider *providersv1alpha1.Provider) string {
	return "platform-mesh-provider-token-" + provider.Name
}

func providerClusterRoleName(provider *providersv1alpha1.Provider) string {
	return "platform-mesh-provider-" + provider.Name
}

// ScopedKubeconfigSubroutine creates the ServiceAccount, RBAC, static SA token
// Secret, and kubeconfig Secret inside the provider workspace in a single
// reconciliation step. Runs in the kcp workspace via the VW-aware client.
type ScopedKubeconfigSubroutine struct {
	// client is the VW-aware client; the reconcile context carries the
	// workspace cluster info so operations are routed to the right workspace.
	client client.Client
	kcpUrl string
}

func NewScopedKubeconfigSubroutine(cl client.Client, kcpUrl string) *ScopedKubeconfigSubroutine {
	return &ScopedKubeconfigSubroutine{
		client: cl,
		kcpUrl: kcpUrl,
	}
}

func (r *ScopedKubeconfigSubroutine) GetName() string {
	return ScopedKubeconfigSubroutineName
}

func (r *ScopedKubeconfigSubroutine) Process(ctx context.Context, obj client.Object) (subroutines.Result, error) {
	log := logger.LoadLoggerFromContext(ctx).ChildLogger("subroutine", r.GetName())
	inst := obj.(*providersv1alpha1.Provider)

	saName := providerServiceAccountName(inst)
	tokenSecretName := providerServiceAccountTokenSecretName(inst)
	kubeconfigSecretName := providerKubeconfigSecretName(inst)
	clusterRoleName := providerClusterRoleName(inst)

	// Ensure the default namespace exists in the workspace.
	ns := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: providerSANamespace}}
	if err := r.client.Create(ctx, ns); err != nil && !kerrors.IsAlreadyExists(err) {
		return subroutines.OK(), gcerrors.Wrap(err, "ensure namespace %s in provider workspace", providerSANamespace)
	}

	// Ensure ServiceAccount.
	sa := &corev1.ServiceAccount{ObjectMeta: metav1.ObjectMeta{Name: saName, Namespace: providerSANamespace}}
	if err := r.client.Create(ctx, sa); err != nil && !kerrors.IsAlreadyExists(err) {
		return subroutines.OK(), gcerrors.Wrap(err, "create ServiceAccount %s", saName)
	}

	// Ensure ClusterRole.
	cr := &rbacv1.ClusterRole{ObjectMeta: metav1.ObjectMeta{Name: clusterRoleName}}
	if _, err := controllerutil.CreateOrUpdate(ctx, r.client, cr, func() error {
		cr.Rules = []rbacv1.PolicyRule{
			// TODO: define exact permission claims required by the provider. ManagedProvider.Spec.PermissionClaims?
			{ // Until we figure this out. 🤩
				APIGroups: []string{"*"},
				Resources: []string{"*"},
				Verbs:     []string{"*"},
			},
		}
		return nil
	}); err != nil {
		return subroutines.OK(), gcerrors.Wrap(err, "create or update ClusterRole %s", clusterRoleName)
	}

	// Ensure ClusterRoleBinding for the provider role.
	crb := &rbacv1.ClusterRoleBinding{ObjectMeta: metav1.ObjectMeta{Name: clusterRoleName}}
	if _, err := controllerutil.CreateOrUpdate(ctx, r.client, crb, func() error {
		crb.RoleRef = rbacv1.RoleRef{APIGroup: rbacv1.GroupName, Kind: "ClusterRole", Name: clusterRoleName}
		crb.Subjects = []rbacv1.Subject{{Kind: rbacv1.ServiceAccountKind, Namespace: providerSANamespace, Name: saName}}
		return nil
	}); err != nil {
		return subroutines.OK(), gcerrors.Wrap(err, "create or update ClusterRoleBinding %s", clusterRoleName)
	}

	// Ensure a static long-lived SA token Secret.
	tokenSecret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: tokenSecretName, Namespace: providerSANamespace}}
	if _, err := controllerutil.CreateOrUpdate(ctx, r.client, tokenSecret, func() error {
		tokenSecret.Type = corev1.SecretTypeServiceAccountToken
		if tokenSecret.Annotations == nil {
			tokenSecret.Annotations = map[string]string{}
		}
		tokenSecret.Annotations[corev1.ServiceAccountNameKey] = saName
		return nil
	}); err != nil {
		return subroutines.OK(), gcerrors.Wrap(err, "create or update token Secret %s", tokenSecretName)
	}
	if len(tokenSecret.Data["token"]) == 0 || len(tokenSecret.Data["ca.crt"]) == 0 {
		log.Info().Str("secret", tokenSecretName).Msg("SA token not yet populated, requeuing")
		return subroutines.StopWithRequeue(waitProviderRequeueDuration, "waiting for SA token to be populated"), nil
	}

	token := string(tokenSecret.Data["token"])
	caData := tokenSecret.Data["ca.crt"]
	hostURL := inst.Spec.HostOverride
	if hostURL == "" {
		hostURL = r.kcpUrl
	}

	kubeconfigBytes, err := clientcmd.Write(buildProviderScopedKubeconfig(hostURL, token, caData))
	if err != nil {
		return subroutines.OK(), gcerrors.Wrap(err, "serialize kubeconfig")
	}

	kubeconfigSecret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: kubeconfigSecretName, Namespace: providerSANamespace}}
	if _, err := controllerutil.CreateOrUpdate(ctx, r.client, kubeconfigSecret, func() error {
		kubeconfigSecret.Data = map[string][]byte{"kubeconfig": kubeconfigBytes}
		return nil
	}); err != nil {
		return subroutines.OK(), gcerrors.Wrap(err, "write kubeconfig Secret %s", kubeconfigSecretName)
	}

	inst.Status.KubeconfigSecretRef = &providersv1alpha1.LocalSecretReference{
		Name: kubeconfigSecretName,
		// TODO: add namespace
	}

	// TODO: move out if we have more subroutines.
	inst.Status.Phase = "Ready"

	log.Info().Str("provider", inst.Name).Str("secret", kubeconfigSecretName).Msg("Ensured scoped kubeconfig in provider workspace")
	return subroutines.OK(), nil
}

func (r *ScopedKubeconfigSubroutine) Finalize(ctx context.Context, obj client.Object) (subroutines.Result, error) {
	inst := obj.(*providersv1alpha1.Provider)
	log := logger.LoadLoggerFromContext(ctx).ChildLogger("subroutine", r.GetName())

	saName := providerServiceAccountName(inst)
	tokenSecretName := providerServiceAccountTokenSecretName(inst)
	kubeconfigSecretName := providerKubeconfigSecretName(inst)
	clusterRoleName := providerClusterRoleName(inst)

	for _, res := range []client.Object{
		&rbacv1.ClusterRoleBinding{ObjectMeta: metav1.ObjectMeta{Name: clusterRoleName}},
		&rbacv1.ClusterRole{ObjectMeta: metav1.ObjectMeta{Name: clusterRoleName}},
		&corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: tokenSecretName, Namespace: providerSANamespace}},
		&corev1.ServiceAccount{ObjectMeta: metav1.ObjectMeta{Name: saName, Namespace: providerSANamespace}},
		&corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: kubeconfigSecretName}},
	} {
		if err := r.client.Delete(ctx, res); err != nil && !kerrors.IsNotFound(err) {
			return subroutines.OK(), gcerrors.Wrap(err, "delete %T %s", res, res.GetName())
		}
	}

	log.Info().Str("provider", inst.Name).Msg("Deleted scoped kubeconfig resources from provider workspace")
	return subroutines.OK(), nil
}

func (r *ScopedKubeconfigSubroutine) Finalizers(_ client.Object) []string {
	return []string{scopedKubeconfigFinalizer}
}

func buildProviderScopedKubeconfig(hostURL, token string, caData []byte) clientcmdapi.Config {
	return clientcmdapi.Config{
		Clusters:       map[string]*clientcmdapi.Cluster{"default-cluster": {Server: hostURL, CertificateAuthorityData: caData}},
		AuthInfos:      map[string]*clientcmdapi.AuthInfo{"default-auth": {Token: token}},
		Contexts:       map[string]*clientcmdapi.Context{"default-context": {Cluster: "default-cluster", AuthInfo: "default-auth"}},
		CurrentContext: "default-context",
	}
}
