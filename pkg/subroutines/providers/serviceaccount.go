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
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	providersv1alpha1 "github.com/platform-mesh/platform-mesh-operator/api/providers/v1alpha1"
)

const ServiceAccountSubroutineName = "ServiceAccountSubroutine"

// ServiceAccountSubroutine creates a ServiceAccount inside the provider
// workspace. The SA is used by the provider controller to authenticate
// against the kcp workspace. Runs in the kcp workspace via the VW client.
type ServiceAccountSubroutine struct {
	// client is the VW-aware client; the reconcile context carries the
	// workspace cluster info so operations are routed to the right workspace.
	client client.Client
}

func NewServiceAccountSubroutine(cl client.Client) *ServiceAccountSubroutine {
	return &ServiceAccountSubroutine{client: cl}
}

func (r *ServiceAccountSubroutine) GetName() string {
	return ServiceAccountSubroutineName
}

func (r *ServiceAccountSubroutine) Process(ctx context.Context, obj client.Object) (subroutines.Result, error) {
	log := logger.LoadLoggerFromContext(ctx).ChildLogger("subroutine", r.GetName())
	inst := obj.(*providersv1alpha1.Provider)

	sa := &corev1.ServiceAccount{}
	err := r.client.Get(ctx, types.NamespacedName{Name: inst.Name}, sa)
	if err != nil && !kerrors.IsNotFound(err) {
		return subroutines.OK(), gcerrors.Wrap(err, "failed to get ServiceAccount %s", inst.Name)
	}

	if kerrors.IsNotFound(err) {
		sa = &corev1.ServiceAccount{
			ObjectMeta: metav1.ObjectMeta{
				Name:      inst.Name,
				Namespace: "default",
			},
		}
		if err := r.client.Create(ctx, sa); err != nil {
			return subroutines.OK(), gcerrors.Wrap(err, "failed to create ServiceAccount %s", inst.Name)
		}
		log.Info().Str("serviceAccount", inst.Name).Msg("Created ServiceAccount in provider workspace")
	}

	return subroutines.OK(), nil
}

func (r *ServiceAccountSubroutine) Finalize(ctx context.Context, obj client.Object) (subroutines.Result, error) {
	inst := obj.(*providersv1alpha1.Provider)
	log := logger.LoadLoggerFromContext(ctx).ChildLogger("subroutine", r.GetName())

	sa := &corev1.ServiceAccount{}
	sa.Name = inst.Name
	if err := r.client.Delete(ctx, sa); err != nil && !kerrors.IsNotFound(err) {
		return subroutines.OK(), gcerrors.Wrap(err, "failed to delete ServiceAccount %s", inst.Name)
	}

	log.Info().Str("serviceAccount", inst.Name).Msg("Deleted ServiceAccount from provider workspace")
	return subroutines.OK(), nil
}

func (r *ServiceAccountSubroutine) Finalizers(_ client.Object) []string {
	return []string{"providers.platform-mesh.io/serviceaccount-finalizer"}
}
