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

	pmconfig "github.com/platform-mesh/golang-commons/config"
	"github.com/platform-mesh/golang-commons/controller/filter"
	"github.com/platform-mesh/golang-commons/controller/lifecycle/ratelimiter"
	"github.com/platform-mesh/subroutines"
	"github.com/platform-mesh/subroutines/conditions"
	"github.com/platform-mesh/subroutines/lifecycle"
	"k8s.io/client-go/util/workqueue"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	mcbuilder "sigs.k8s.io/multicluster-runtime/pkg/builder"
	mcmanager "sigs.k8s.io/multicluster-runtime/pkg/manager"
	mcreconcile "sigs.k8s.io/multicluster-runtime/pkg/reconcile"

	providersv1alpha1 "github.com/platform-mesh/platform-mesh-operator/api/providers/v1alpha1"
	"github.com/platform-mesh/platform-mesh-operator/internal/config"
)

const ManagedProvidercontrollerName = "ManagedProviderReconciler"

var (
	pmReconcilerName = "PlatformMeshReconciler"
)

// ManagedProviderReconciler reconciles a ManagedProvider object
type ManagedProviderReconciler struct {
	lifecycle   *lifecycle.Lifecycle
	rateLimiter workqueue.TypedRateLimiter[mcreconcile.Request]
}

// +kubebuilder:rbac:groups=providers.platform-mesh.io,resources=managedproviders,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=providers.platform-mesh.io,resources=managedproviders/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=providers.platform-mesh.io,resources=managedproviders/finalizers,verbs=update

// Reconcile is part of the main kubernetes reconciliation loop which aims to
// move the current state of the cluster closer to the desired state.
// TODO(user): Modify the Reconcile function to compare the state specified by
// the ManagedProvider object against the actual cluster state, and then
// perform operations to make the cluster state reflect the state specified by
// the user.
//
// For more details, check Reconcile and its Result here:
// - https://pkg.go.dev/sigs.k8s.io/controller-runtime@v0.23.3/pkg/reconcile
func (r *ManagedProviderReconciler) Reconcile(ctx context.Context, req mcreconcile.Request) (ctrl.Result, error) {
	// TODO(user): your logic here

	return ctrl.Result{}, nil
}

// SetupWithManager sets up the controller with the Manager.
func (r *ManagedProviderReconciler) SetupWithManager(mgr mcmanager.Manager, cfg *pmconfig.CommonServiceConfig,
	eventPredicates ...predicate.Predicate) error {
	opts := controller.TypedOptions[mcreconcile.Request]{
		MaxConcurrentReconciles: cfg.MaxConcurrentReconciles,
		RateLimiter:             r.rateLimiter,
	}
	predicates := append([]predicate.Predicate{filter.DebugResourcesBehaviourPredicate(cfg.DebugLabelValue)}, eventPredicates...)
	return mcbuilder.ControllerManagedBy(mgr).
		Named(pmReconcilerName).
		For(&providersv1alpha1.ManagedProvider{}).
		WithOptions(opts).
		WithEventFilter(predicate.And(predicates...)).
		Complete(r)
}

func NewManagedProviderReconciler(mgr mcmanager.Manager, cfg *config.OperatorConfig, commonCfg *pmconfig.CommonServiceConfig, dir string) (*ManagedProviderReconciler, error) {
	kcpUrl := fmt.Sprintf("https://%s-front-proxy.%s:%s", cfg.KCP.FrontProxyName, cfg.KCP.Namespace, cfg.KCP.FrontProxyPort)
	if cfg.KCP.Url != "" {
		kcpUrl = cfg.KCP.Url
	}
	_ = kcpUrl

	localCl := mgr.GetLocalManager().GetClient()
	_ = localCl

	var subs []subroutines.Subroutine
	/*if cfg.Subroutines.Deployment.Enabled {
		subs = append(subs, pmsubs.NewDeploymentSubroutine(localCl, commonCfg, cfg))
	}
	if cfg.Subroutines.KcpSetup.Enabled {
		subs = append(subs, pmsubs.NewKcpsetupSubroutine(localCl, &pmsubs.Helper{}, cfg, dir+"/manifests/kcp", kcpUrl))
	}
	if cfg.Subroutines.ProviderSecret.Enabled {
		subs = append(subs, pmsubs.NewProviderSecretSubroutine(localCl, &pmsubs.Helper{}, pmsubs.DefaultHelmGetter{}, kcpUrl))
	}
	if cfg.Subroutines.FeatureToggles.Enabled {
		subs = append(subs, pmsubs.NewFeatureToggleSubroutine(localCl, &pmsubs.Helper{}, cfg, kcpUrl))
	}
	if cfg.Subroutines.Wait.Enabled {
		subs = append(subs, pmsubs.NewWaitSubroutine(localCl, &pmsubs.Helper{}, cfg, kcpUrl))
	}*/

	rl, err := ratelimiter.NewStaticThenExponentialRateLimiter[mcreconcile.Request](ratelimiter.NewConfig())
	if err != nil {
		return nil, fmt.Errorf("creating rate limiter: %w", err)
	}

	lc := lifecycle.New(mgr, pmReconcilerName, func() client.Object {
		return &providersv1alpha1.ManagedProvider{}
	}, subs...).WithConditions(conditions.NewManager())

	return &ManagedProviderReconciler{
		lifecycle:   lc,
		rateLimiter: rl,
	}, nil
}
