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
	rbacv1 "k8s.io/api/rbac/v1"
	kerrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	providersv1alpha1 "github.com/platform-mesh/platform-mesh-operator/api/providers/v1alpha1"
)

const RBACSubroutineName = "RBACSubroutine"

// RBACSubroutine creates a ClusterRole and ClusterRoleBinding in the provider
// workspace granting the provider ServiceAccount the permissions it needs.
// Runs in the kcp workspace via the VW client.
type RBACSubroutine struct {
	// client is the VW-aware client; the reconcile context carries the
	// workspace cluster info so operations are routed to the right workspace.
	client client.Client
}

func NewRBACSubroutine(cl client.Client) *RBACSubroutine {
	return &RBACSubroutine{client: cl}
}

func (r *RBACSubroutine) GetName() string {
	return RBACSubroutineName
}

func (r *RBACSubroutine) Process(ctx context.Context, obj client.Object) (subroutines.Result, error) {
	log := logger.LoadLoggerFromContext(ctx).ChildLogger("subroutine", r.GetName())
	inst := obj.(*providersv1alpha1.Provider)

	if err := r.ensureClusterRole(ctx, inst); err != nil {
		return subroutines.OK(), err
	}
	if err := r.ensureClusterRoleBinding(ctx, inst); err != nil {
		return subroutines.OK(), err
	}

	log.Info().Str("provider", inst.Name).Msg("Ensured RBAC in provider workspace")
	return subroutines.OK(), nil
}

func (r *RBACSubroutine) ensureClusterRole(ctx context.Context, inst *providersv1alpha1.Provider) error {
	cr := &rbacv1.ClusterRole{}
	err := r.client.Get(ctx, types.NamespacedName{Name: inst.Name}, cr)
	if err != nil && !kerrors.IsNotFound(err) {
		return gcerrors.Wrap(err, "failed to get ClusterRole %s", inst.Name)
	}

	if kerrors.IsNotFound(err) {
		cr = &rbacv1.ClusterRole{
			ObjectMeta: metav1.ObjectMeta{Name: inst.Name},
			Rules: []rbacv1.PolicyRule{
				// Follows providers.platform-mesh.io permission claims.
				{
					APIGroups: []string{""},
					Resources: []string{"serviceaccounts"},
					Verbs:     []string{"get", "list", "watch", "create", "update", "patch", "delete"},
				},
				{
					APIGroups: []string{""},
					Resources: []string{"secrets"},
					Verbs:     []string{"get", "list", "watch", "create", "update", "patch", "delete"},
				},
				{
					APIGroups: []string{""},
					Resources: []string{"roles"},
					Verbs:     []string{"get", "list", "watch", "create", "update", "patch", "delete"},
				},
				{
					APIGroups: []string{""},
					Resources: []string{"rolebindings"},
					Verbs:     []string{"get", "list", "watch", "create", "update", "patch", "delete"},
				},
				{
					APIGroups: []string{""},
					Resources: []string{"rolebindings"},
					Verbs:     []string{"get", "list", "watch", "create", "update", "patch", "delete"},
				},
			},
		}
		if err := r.client.Create(ctx, cr); err != nil {
			return gcerrors.Wrap(err, "failed to create ClusterRole %s", inst.Name)
		}
	}
	return nil
}

func (r *RBACSubroutine) ensureClusterRoleBinding(ctx context.Context, inst *providersv1alpha1.Provider) error {
	crb := &rbacv1.ClusterRoleBinding{}
	err := r.client.Get(ctx, types.NamespacedName{Name: inst.Name}, crb)
	if err != nil && !kerrors.IsNotFound(err) {
		return gcerrors.Wrap(err, "failed to get ClusterRoleBinding %s", inst.Name)
	}

	if kerrors.IsNotFound(err) {
		crb = &rbacv1.ClusterRoleBinding{
			ObjectMeta: metav1.ObjectMeta{Name: inst.Name},
			RoleRef: rbacv1.RoleRef{
				APIGroup: rbacv1.GroupName,
				Kind:     "ClusterRole",
				Name:     inst.Name,
			},
			Subjects: []rbacv1.Subject{
				{
					Kind:      rbacv1.ServiceAccountKind,
					Name:      inst.Name,
					Namespace: "default",
				},
			},
		}
		if err := r.client.Create(ctx, crb); err != nil {
			return gcerrors.Wrap(err, "failed to create ClusterRoleBinding %s", inst.Name)
		}
	}
	return nil
}

func (r *RBACSubroutine) Finalize(ctx context.Context, obj client.Object) (subroutines.Result, error) {
	inst := obj.(*providersv1alpha1.Provider)
	log := logger.LoadLoggerFromContext(ctx).ChildLogger("subroutine", r.GetName())

	crb := &rbacv1.ClusterRoleBinding{}
	crb.Name = inst.Name
	if err := r.client.Delete(ctx, crb); err != nil && !kerrors.IsNotFound(err) {
		return subroutines.OK(), gcerrors.Wrap(err, "failed to delete ClusterRoleBinding %s", inst.Name)
	}

	cr := &rbacv1.ClusterRole{}
	cr.Name = inst.Name
	if err := r.client.Delete(ctx, cr); err != nil && !kerrors.IsNotFound(err) {
		return subroutines.OK(), gcerrors.Wrap(err, "failed to delete ClusterRole %s", inst.Name)
	}

	log.Info().Str("provider", inst.Name).Msg("Deleted RBAC from provider workspace")
	return subroutines.OK(), nil
}

func (r *RBACSubroutine) Finalizers(_ client.Object) []string {
	return []string{"providers.platform-mesh.io/rbac-finalizer"}
}
