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
	kerrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	providersv1alpha1 "github.com/platform-mesh/platform-mesh-operator/api/providers/v1alpha1"
)

const KubeconfigSecretSubroutineName = "KubeconfigSecretSubroutine"

// KubeconfigSecretSubroutine generates a kubeconfig Secret inside the provider
// workspace scoped to the workspace's logical cluster name, and records its
// name in status.kubeconfigSecretRef. Runs in the kcp workspace via the VW client.
type KubeconfigSecretSubroutine struct {
	// client is the VW-aware client; the reconcile context carries the
	// workspace cluster info so operations are routed to the right workspace.
	client client.Client
}

func NewKubeconfigSecretSubroutine(cl client.Client) *KubeconfigSecretSubroutine {
	return &KubeconfigSecretSubroutine{client: cl}
}

func (r *KubeconfigSecretSubroutine) GetName() string {
	return KubeconfigSecretSubroutineName
}

func (r *KubeconfigSecretSubroutine) Process(ctx context.Context, obj client.Object) (subroutines.Result, error) {
	log := logger.LoadLoggerFromContext(ctx).ChildLogger("subroutine", r.GetName())
	inst := obj.(*providersv1alpha1.Provider)

	saTokenSecretName := providerServiceAccountTokenSecretName(inst)
	kubeconfigSecretName := providerKubeconfigSecretName(inst)

	tokenSecret := corev1.Secret{}
	if err := r.client.Get(ctx, saTokenSecretName, &tokenSecret); err != nil {
		return subroutines.OK(), gcerrors.Wrap(err, "failed to get service account token Secret %s", saTokenSecretName)
	}

	kubeconfigSecret := &corev1.Secret{}
	err := r.client.Get(ctx, kubeconfigSecretName, kubeconfigSecret)
	if err != nil && !kerrors.IsNotFound(err) {
		return subroutines.OK(), gcerrors.Wrap(err, "failed to get kubeconfig Secret %s", kubeconfigSecretName)
	}

	if kerrors.IsNotFound(err) {
		// TODO: generate kubeconfig from the workspace logical cluster name
		// (not the human-readable path, per RFC 006 §Authentication) and the
		// SA token. Use spec.hostOverride if set, otherwise the operator-configured
		// front-proxy URL.
		secret = &corev1.Secret{}
		secret.Name = secretName
		// secret.Data = map[string][]byte{"kubeconfig": <generated>}
		if err := r.client.Create(ctx, secret); err != nil {
			return subroutines.OK(), gcerrors.Wrap(err, "failed to create kubeconfig Secret %s", secretName)
		}
		log.Info().Str("secret", secretName).Msg("Created kubeconfig Secret in provider workspace")
	}

	// TODO: update inst.Status.KubeconfigSecretRef and inst.Status.Phase = "Ready"
	// via r.client.Status().Update(ctx, inst)

	return subroutines.OK(), nil
}

func (r *KubeconfigSecretSubroutine) Finalize(ctx context.Context, obj client.Object) (subroutines.Result, error) {
	inst := obj.(*providersv1alpha1.Provider)
	log := logger.LoadLoggerFromContext(ctx).ChildLogger("subroutine", r.GetName())

	secretName := inst.Name + "-kubeconfig"
	secret := &corev1.Secret{}
	secret.Name = secretName
	if err := r.client.Delete(ctx, secret); err != nil && !kerrors.IsNotFound(err) {
		return subroutines.OK(), gcerrors.Wrap(err, "failed to delete kubeconfig Secret %s", secretName)
	}

	log.Info().Str("secret", secretName).Msg("Deleted kubeconfig Secret from provider workspace")
	return subroutines.OK(), nil
}

func (r *KubeconfigSecretSubroutine) Finalizers(_ client.Object) []string {
	return []string{"providers.platform-mesh.io/kubeconfig-secret-finalizer"}
}
