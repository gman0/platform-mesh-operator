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
	"fmt"

	gcerrors "github.com/platform-mesh/golang-commons/errors"
	"github.com/platform-mesh/golang-commons/logger"
	"github.com/platform-mesh/subroutines"
	corev1 "k8s.io/api/core/v1"
	kerrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	providersv1alpha1 "github.com/platform-mesh/platform-mesh-operator/api/providers/v1alpha1"
)

const ServiceAccountTokenSubroutineName = "ServiceAccountTokenSubroutine"

// ServiceAccountTokenSubroutine creates a long-lived token Secret of type
// kubernetes.io/service-account-token for the provider ServiceAccount.
// Required because Kubernetes no longer auto-creates SA tokens since v1.24.
// Must run after ServiceAccountSubroutine and before KubeconfigSecretSubroutine,
// which reads the token to build the kubeconfig.
type ServiceAccountTokenSubroutine struct {
	// client is the VW-aware client; the reconcile context carries the
	// workspace cluster info so operations are routed to the right workspace.
	client client.Client
}

func NewServiceAccountTokenSubroutine(cl client.Client) *ServiceAccountTokenSubroutine {
	return &ServiceAccountTokenSubroutine{client: cl}
}

func (r *ServiceAccountTokenSubroutine) GetName() string {
	return ServiceAccountTokenSubroutineName
}

func (r *ServiceAccountTokenSubroutine) Process(ctx context.Context, obj client.Object) (subroutines.Result, error) {
	log := logger.LoadLoggerFromContext(ctx).ChildLogger("subroutine", r.GetName())
	inst := obj.(*providersv1alpha1.Provider)

	nsName := providerServiceAccountTokenSecretName(inst)
	secret := &corev1.Secret{}
	err := r.client.Get(ctx, nsName, secret)
	if err != nil && !kerrors.IsNotFound(err) {
		return subroutines.OK(), gcerrors.Wrap(err, "failed to get token Secret %s", nsName)
	}

	if kerrors.IsNotFound(err) {
		secret = &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{
				Name:      nsName.Name,
				Namespace: nsName.Namespace,
				Annotations: map[string]string{
					corev1.ServiceAccountNameKey: inst.Name,
				},
			},
			Type: corev1.SecretTypeServiceAccountToken,
		}
		if err := r.client.Create(ctx, secret); err != nil {
			return subroutines.OK(), gcerrors.Wrap(err, "failed to create token Secret %s", nsName)
		}
		log.Info().Str("secret", nsName.String()).Msg("Created ServiceAccount token Secret in provider workspace")
	}

	if secret.Data == nil || len(secret.Data["token"]) == 0 {
		return subroutines.StopWithRequeue(waitProviderRequeueDuration,
			fmt.Sprintf("waiting for service account token %s to become populated", nsName)), nil
	}

	return subroutines.OK(), nil
}

func (r *ServiceAccountTokenSubroutine) Finalize(ctx context.Context, obj client.Object) (subroutines.Result, error) {
	inst := obj.(*providersv1alpha1.Provider)
	log := logger.LoadLoggerFromContext(ctx).ChildLogger("subroutine", r.GetName())

	nsName := providerServiceAccountTokenSecretName(inst)
	secret := &corev1.Secret{}
	secret.Name = nsName.Name
	secret.Namespace = nsName.Namespace
	if err := r.client.Delete(ctx, secret); err != nil && !kerrors.IsNotFound(err) {
		return subroutines.OK(), gcerrors.Wrap(err, "failed to delete token Secret %s", nsName)
	}

	log.Info().Str("secret", nsName.String()).Msg("Deleted ServiceAccount token Secret from provider workspace")
	return subroutines.OK(), nil
}

func (r *ServiceAccountTokenSubroutine) Finalizers(_ client.Object) []string {
	return []string{"providers.platform-mesh.io/serviceaccount-token-finalizer"}
}
