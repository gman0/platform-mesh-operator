/*
Copyright 2024.

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

package cmd

import (
	"context"
	"crypto/tls"
	"fmt"
	"net/http"
	"os"
	"time"

	"github.com/kcp-dev/multicluster-provider/apiexport"
	pmcontext "github.com/platform-mesh/golang-commons/context"
	"github.com/spf13/cobra"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	corev1 "k8s.io/api/core/v1"
	_ "k8s.io/client-go/plugin/pkg/client/auth"
	"k8s.io/client-go/rest"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/cluster"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
	mcmanager "sigs.k8s.io/multicluster-runtime/pkg/manager"
	"sigs.k8s.io/multicluster-runtime/pkg/multicluster"
	"sigs.k8s.io/multicluster-runtime/providers/multi"

	"github.com/platform-mesh/golang-commons/traces"

	"github.com/platform-mesh/platform-mesh-operator/internal/config"
	"github.com/platform-mesh/platform-mesh-operator/internal/controller"
	"github.com/platform-mesh/platform-mesh-operator/internal/controller/providers"
	"github.com/platform-mesh/platform-mesh-operator/pkg/subroutines"
)

var operatorCmd = &cobra.Command{
	Use:   "operator",
	Short: "operator to setup platform-mesh",
	Run:   RunController,
}

func buildKcpAdminConfigForWorkspace(cl client.Client, kcp config.KCPConfig, wsPath string) (*rest.Config, error) {
	kcpUrl := kcp.Url
	if kcpUrl == "" {
		kcpUrl = fmt.Sprintf("https://%s-front-proxy.%s:%s", kcp.FrontProxyName, kcp.Namespace, kcp.FrontProxyPort)
	}
	kcpUrl += fmt.Sprintf("/clusters/%s", wsPath)
	return subroutines.BuildKubeconfigFromConfig(cl, &kcp, kcpUrl)
}

func RunController(_ *cobra.Command, _ []string) { // coverage-ignore
	var err error

	ctrl.SetLogger(log.ComponentLogger("controller-runtime").Logr())

	log.Info().Msg("Starting PlatformMesh Operator")
	defer log.Info().Msg("Shutting down PlatformMesh Operator")

	ctx, _, shutdown := pmcontext.StartContext(log, operatorCfg, defaultCfg.ShutdownTimeout)
	defer shutdown()

	disableHTTP2 := func(c *tls.Config) {
		log.Info().Msg("disabling http/2")
		c.NextProtos = []string{"http/1.1"}
	}

	tlsOpts := []func(*tls.Config){}
	if !defaultCfg.EnableHTTP2 {
		tlsOpts = append(tlsOpts, disableHTTP2)
	}

	var providerShutdown func(ctx context.Context) error
	if defaultCfg.Tracing.Enabled {
		providerShutdown, err = traces.InitProvider(ctx, defaultCfg.Tracing.Collector)
		if err != nil {
			log.Fatal().Err(err).Msg("unable to start gRPC-Sidecar TracerProvider")
		}
	} else {
		providerShutdown, err = traces.InitLocalProvider(ctx, defaultCfg.Tracing.Collector, false)
		if err != nil {
			log.Fatal().Err(err).Msg("unable to start local TracerProvider")
		}
	}

	defer func() {
		if err := providerShutdown(ctx); err != nil {
			log.Fatal().Err(err).Msg("failed to shutdown TracerProvider")
		}
	}()

	log.Info().Msg("Starting manager")

	restCfg := ctrl.GetConfigOrDie()
	if operatorCfg.RemoteRuntime.IsEnabled() {
		setupLog.Info("Remote PlatformMesh reconciliation enabled, kubeconfig: " + operatorCfg.RemoteRuntime.Kubeconfig)
		var err error
		_, restCfg, err = subroutines.GetClientAndRestConfig(operatorCfg.RemoteRuntime.Kubeconfig)
		if err != nil {
			setupLog.Error(err, "unable to create PlatformMesh client")
			os.Exit(1)
		}
	}
	setupLog.Info(fmt.Sprintf("PlatformMesh Host: %s", restCfg.Host))
	restCfg.Wrap(func(rt http.RoundTripper) http.RoundTripper {
		return otelhttp.NewTransport(rt)
	})

	var leaderCfg *rest.Config
	if defaultCfg.LeaderElectionEnabled {
		leaderCfg, err = rest.InClusterConfig()
		if err != nil {
			log.Fatal().Err(err).Msg("unable to get in-cluster config")
		}
	}

	multiProvider := multi.New(multi.Options{})

	mgr, err := mcmanager.New(restCfg, multiProvider, mcmanager.Options{
		Scheme: scheme,
		Metrics: metricsserver.Options{
			BindAddress:   defaultCfg.Metrics.BindAddress,
			SecureServing: defaultCfg.Metrics.Secure,
			TLSOpts:       tlsOpts,
		},
		BaseContext:                   func() context.Context { return ctx },
		HealthProbeBindAddress:        defaultCfg.HealthProbeBindAddress,
		LeaderElection:                defaultCfg.LeaderElectionEnabled,
		LeaderElectionID:              "81924e50.platform-mesh.org",
		LeaderElectionConfig:          leaderCfg,
		LeaderElectionReleaseOnCancel: true,
	})
	if err != nil {
		setupLog.Error(err, "unable to start manager")
		os.Exit(1)
	}

	log.Info().Msg("Manager successfully created")

	localClient := mgr.GetLocalManager().GetClient()

	restCfgInfra := ctrl.GetConfigOrDie()
	restCfgInfra.Wrap(func(rt http.RoundTripper) http.RoundTripper {
		return otelhttp.NewTransport(rt)
	})
	clientInfra, err := client.New(restCfgInfra, client.Options{Scheme: subroutines.GetClientScheme()})
	if err != nil {
		setupLog.Error(err, "unable to create Infra client")
		os.Exit(1)
	}
	if operatorCfg.RemoteInfra.IsEnabled() {
		var infraErr error
		clientInfra, _, infraErr = subroutines.GetClientAndRestConfig(operatorCfg.RemoteInfra.Kubeconfig)
		if infraErr != nil {
			setupLog.Error(infraErr, "unable to create Infra client")
			os.Exit(1)
		}
	}
	imageVersionStore := subroutines.NewImageVersionStore()

	pmReconciler, err := controller.NewPlatformMeshReconciler(mgr, &operatorCfg, defaultCfg, operatorCfg.WorkspaceDir, clientInfra, imageVersionStore)
	if err != nil {
		setupLog.Error(err, "unable to create PlatformMesh reconciler")
		os.Exit(1)
	}
	if err := pmReconciler.SetupWithManager(mgr, defaultCfg); err != nil {
		setupLog.Error(err, "unable to create controller", "controller", "PlatformMesh")
		os.Exit(1)
	}

	resourceReconciler, err := controller.NewResourceReconciler(mgr, &operatorCfg, clientInfra, imageVersionStore)
	if err != nil {
		setupLog.Error(err, "unable to create Resource reconciler")
		os.Exit(1)
	}
	if err := resourceReconciler.SetupWithManager(mgr, defaultCfg); err != nil {
		setupLog.Error(err, "unable to create controller", "controller", "Resource")
		os.Exit(1)
	}

	managedProvidersReconciler, err := providers.NewManagedProviderReconciler(mgr, &operatorCfg, defaultCfg)
	if err != nil {
		setupLog.Error(err, "unable to create ManagedProvider reconciler")
		os.Exit(1)
	}
	if err := managedProvidersReconciler.SetupWithManager(mgr, defaultCfg); err != nil {
		setupLog.Error(err, "unable to create controller", "controller", "ManagedProvider")
		os.Exit(1)
	}

	providerReconciler, err := providers.NewProviderReconciler(mgr, &operatorCfg, defaultCfg, localClient)
	if err != nil {
		setupLog.Error(err, "unable to create ProviderReconciler")
		os.Exit(1)
	}
	if err := providerReconciler.SetupWithManager(mgr, defaultCfg); err != nil {
		setupLog.Error(err, "unable to create controller", "controller", "Provider")
		os.Exit(1)
	}

	if err := mgr.Add(&kcpRunnable{
		multiProvider: multiProvider,
		localClient:   localClient,
		cfg:           &operatorCfg,
	}); err != nil {
		setupLog.Error(err, "unable to add KCP provider runnable")
		os.Exit(1)
	}

	if err := mgr.AddHealthzCheck("healthz", healthz.Ping); err != nil {
		setupLog.Error(err, "unable to set up health check")
		os.Exit(1)
	}
	if err := mgr.AddReadyzCheck("readyz", healthz.Ping); err != nil {
		setupLog.Error(err, "unable to set up ready check")
		os.Exit(1)
	}

	setupLog.Info("starting manager")
	if err := mgr.Start(ctrl.SetupSignalHandler()); err != nil {
		log.Fatal().Err(err).Msg("problem running manager")
	}
}

type kcpRunnable struct {
	multiProvider *multi.Provider
	localClient   client.Client
	cfg           *config.OperatorConfig
}

func (r *kcpRunnable) Engage(_ context.Context, _ multicluster.ClusterName, _ cluster.Cluster) error {
	return nil
}

func (r *kcpRunnable) Start(ctx context.Context) error {
	secretKey := client.ObjectKey{
		Namespace: r.cfg.KCP.Namespace,
		Name:      r.cfg.KCP.ClusterAdminSecretName,
	}
	for {
		if err := r.waitForSecret(ctx, secretKey); err != nil {
			return err
		}
		endpointCfg, err := buildKcpAdminConfigForWorkspace(r.localClient, r.cfg.KCP, r.cfg.Providers.ProvidersAPIExportEndpointSliceWorkspace)
		if err != nil {
			setupLog.Error(err, "unable to build KCP admin config, retrying")
			if err := kcpSleep(ctx, 10*time.Second); err != nil {
				return err
			}
			continue
		}
		apiexportProvider, err := apiexport.New(endpointCfg, r.cfg.Providers.ProvidersAPIExportEndpointSliceName, apiexport.Options{
			Scheme: scheme,
		})
		if err != nil {
			setupLog.Error(err, "unable to create apiexport provider, retrying")
			if err := kcpSleep(ctx, 10*time.Second); err != nil {
				return err
			}
			continue
		}
		if err := r.multiProvider.AddProvider("kcp", apiexportProvider); err != nil {
			setupLog.Error(err, "unable to add KCP provider, retrying")
			if err := kcpSleep(ctx, 10*time.Second); err != nil {
				return err
			}
			continue
		}
		if err := r.waitForProviderRemoval(ctx); err != nil {
			return err
		}
	}
}

func (r *kcpRunnable) waitForSecret(ctx context.Context, key client.ObjectKey) error {
	for {
		secret := &corev1.Secret{}
		if err := r.localClient.Get(ctx, key, secret); err == nil {
			return nil
		}
		setupLog.Info("waiting for KCP admin secret", "secret", key)
		if err := kcpSleep(ctx, 10*time.Second); err != nil {
			return err
		}
	}
}

func (r *kcpRunnable) waitForProviderRemoval(ctx context.Context) error {
	for {
		if _, ok := r.multiProvider.GetProvider("kcp"); !ok {
			return nil
		}
		if err := kcpSleep(ctx, 5*time.Second); err != nil {
			return err
		}
	}
}

func kcpSleep(ctx context.Context, d time.Duration) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(d):
		return nil
	}
}
