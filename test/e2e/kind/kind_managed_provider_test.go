package e2e

import (
	"context"
	"time"

	// appsv1 "k8s.io/api/apps/v1"
	kcptenancyv1alpha "github.com/kcp-dev/kcp/sdk/apis/tenancy/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	kerrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/client"

	providersv1alpha1 "github.com/platform-mesh/platform-mesh-operator/api/providers/v1alpha1"
	pmsubs "github.com/platform-mesh/platform-mesh-operator/pkg/subroutines"
)

func (s *KindTestSuite) TestManagedProvider() {
	ctx := s.T().Context()

	s.Run("Ensure life-cycling ManagedProvider works", func() {
		// kcp client scoped to :root:providers:my-managed-provider.
		kcpAdminCfg, err := pmsubs.BuildKcpAdminConfig(s.client, &defaultKcpOperatorConfig, defaultKcpOperatorConfig.Url)
		s.NoError(err, "getting kcp admin rest config should succeed")
		providerScopedKcpAdminCfg := rest.CopyConfig(kcpAdminCfg)
		providerScopedKcpAdminCfg.Host += "/clusters/root:providers:my-managed-provider"
		providerScopedKcpAdminClient, err := client.New(providerScopedKcpAdminCfg, client.Options{
			Scheme: s.scheme,
		})

		// This test life-cycles ManagedProvider twice, validating ManagedProvider.spec.cleanupOnDelete.
		// In both cases, ManagedProvider is expected to create a Deployment in the runtime cluster,
		// and a kubeconfig scoped to provider's workspace.

		ns := corev1.Namespace{
			ObjectMeta: metav1.ObjectMeta{
				Name: "e2e-managed-provider",
			},
		}
		s.logger.Info().Msgf("Creating namespace %q", ns.Name)
		err = s.client.Create(ctx, &ns)
		s.NoError(err, "creating namespace for a ManagedProvider should succeed")
		s.T().Cleanup(func() {
			// s.client.Delete(s.T().Context(), &ns)
		})
		s.logger.Info().Msgf("Namespace %q created", ns.Name)

		// First variant, with cleanupOnDelete=false. Only runtime resources are expected to be deleted on ManagedProvider deletion.

		waitForManagedProviderAndValidate(ctx, s, providerScopedKcpAdminClient, func(mp *providersv1alpha1.ManagedProvider) {
			mp.Spec.CleanupOnDelete = false
		})
		// Deleting ManagedProvider should NOT delete its artifacts on kcp side since spec.cleanupOnDelete=false.
		// The Deployment should be deleted though.
		s.logger.Info().Msgf("Deleting ManagedProvider with cleanupOnDelete=false")
		managedProvider := providersv1alpha1.ManagedProvider{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: "e2e-managed-provider",
				Name:      "my-managed-provider",
			},
		}
		err = s.client.Delete(ctx, &managedProvider)
		s.NoError(err, "deleting ManagedProvider should succeed")
		s.Eventually(func() bool {
			err = s.client.Get(ctx, types.NamespacedName{
				Name:      "my-managed-provider",
				Namespace: ns.Name,
			}, &managedProvider)
			return kerrors.IsNotFound(err)
		}, 240*time.Second, 5*time.Second, "waiting for ManagedProvider to be deleted, but has err=%q", err)

		var provider providersv1alpha1.Provider
		err = providerScopedKcpAdminClient.Get(ctx, types.NamespacedName{
			Namespace: "default",
			Name:      "my-managed-provider",
		}, &provider)
		s.NoError(err, "getting Provider with scopedKcpAdminClient should succeed")
		s.Equal("Ready", provider.Status.Phase, "Provider on kcp side should have reached Phase=Ready")
		s.Nil(provider.DeletionTimestamp, "Provider should not be marked for deletion")

		// Second variant, with cleanupOnDelete=true. Everything is expected to be gone once ManagedProvider is deleted.

		s.logger.Info().Msgf("Re-creating ManagedProvider with cleanupOnDelete=true")
		waitForManagedProviderAndValidate(ctx, s, providerScopedKcpAdminClient, func(mp *providersv1alpha1.ManagedProvider) {
			mp.Spec.CleanupOnDelete = true
		})
		s.logger.Info().Msgf("Deleting ManagedProvider with cleanupOnDelete=true")
		err = s.client.Delete(ctx, &providersv1alpha1.ManagedProvider{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: "e2e-managed-provider",
				Name:      "my-managed-provider",
			},
		})
		s.NoError(err, "deleting ManagedProvider should succeed")
		s.logger.Info().Msgf("ManagedProvider deleted, checking ")
		s.Eventually(func() bool {
			err = s.client.Get(ctx, types.NamespacedName{
				Namespace: "e2e-managed-provider",
				Name:      "my-managed-provider",
			}, &providersv1alpha1.ManagedProvider{})
			return kerrors.IsNotFound(err)
		}, 240*time.Second, 5*time.Second, "waiting for ManagedProvider to be deleted, but has err=%q", err)
		providersScopedAdminCfg := rest.CopyConfig(kcpAdminCfg)
		providersScopedAdminCfg.Host += "/clusters/root:providers"
		s.Eventually(func() bool {
			err = s.client.Get(ctx, types.NamespacedName{
				Name: "my-managed-provider",
			}, &kcptenancyv1alpha.Workspace{})
			return kerrors.IsNotFound(err)
		}, 240*time.Second, 5*time.Second, "waiting for provider's workspace :root:providers:my-managed-provider to be deleted, but has err=%q", err)
	})
}

func waitForManagedProviderAndValidate(ctx context.Context, s *KindTestSuite, providersScopedKcpAdminClient client.Client, patchManagedProviderCreate func(*providersv1alpha1.ManagedProvider)) {
	s.T().Helper()

	managedProviderName := types.NamespacedName{
		Namespace: "e2e-managed-provider",
		Name:      "my-managed-provider",
	}
	managedProvider := providersv1alpha1.ManagedProvider{
		ObjectMeta: metav1.ObjectMeta{
			Name:      managedProviderName.Name,
			Namespace: managedProviderName.Namespace,
		},
		Spec: providersv1alpha1.ManagedProviderSpec{
			Controller: providersv1alpha1.ProviderComponentSpec{
				OCM: providersv1alpha1.OCMComponentSpec{
					ComponentName: "example-httpbin-operator",
					Registry:      "ghcr.io/platform-mesh/helm-charts",
					Version:       "0.5.14",
				},
			},
		},
	}
	patchManagedProviderCreate(&managedProvider)

	s.logger.Info().Msgf("Creating ManagedProvider %q", managedProvider.Name)
	err := s.client.Create(ctx, &managedProvider)
	s.NoError(err, "creating ManagedProvider should succeed")
	s.logger.Info().Msgf("ManagedProvider %q created", managedProvider.Name)

	s.logger.Info().Msgf("Waiting until ManagedProvider has its Status.KubeconfigSecretRef populated")
	s.Eventually(func() bool {
		err := s.client.Get(ctx, managedProviderName, &managedProvider)
		if err != nil {
			return false
		}
		return managedProvider.Status.KubeconfigSecretRef != nil &&
			managedProvider.Status.KubeconfigSecretRef.Name == "platform-mesh-provider-kubeconfig-my-managed-provider" &&
			managedProvider.Status.KubeconfigSecretRef.Namespace == managedProviderName.Namespace
	}, 240*time.Second, 5*time.Second, "waiting for kubeconfig ref in ManagedProvider")
	s.logger.Info().Msgf("ManagedProvider has its Status.KubeconfigSecretRef populated")

	// At this point the Provider on kcp side should be in Phase=Ready.

	s.logger.Info().Msgf("Validate Provider on kcp side")
	var provider providersv1alpha1.Provider
	err = providersScopedKcpAdminClient.Get(ctx, types.NamespacedName{
		Namespace: "default",
		Name:      "my-managed-provider",
	}, &provider)
	s.NoError(err, "getting Provider with scopedKcpAdminClient should succeed")
	s.Equal("Ready", provider.Status.Phase, "Provider on kcp side should have reached Phase=Ready")
	s.NotNil(provider.Status.KubeconfigSecretRef, "Provider should have its KubeconfigSecretRef populated")
	s.Equal("platform-mesh-provider-kubeconfig-my-managed-provider", provider.Status.KubeconfigSecretRef.Name, "Provider has unexpected kubeconfig secret name")
	s.Equal("default", provider.Status.KubeconfigSecretRef.Namespace, "Provider has unexpected kubeconfig secret namespace")

	// Validate provider kubeconfigs on kcp and kind side.

	s.logger.Info().Msgf("Getting provider kubeconfig secret on kcp side")
	var providerKubeconfigSecretInKcp corev1.Secret
	err = providersScopedKcpAdminClient.Get(ctx, types.NamespacedName{
		Namespace: "default",
		Name:      "platform-mesh-provider-kubeconfig-my-managed-provider",
	}, &providerKubeconfigSecretInKcp)
	s.NoError(err, "getting provider kubeconfig secret using scopedKcpAdminClient should succeed")

	s.logger.Info().Msgf("Getting provider kubeconfig secret on runtime sice")
	var providerKubeconfigSecretInKind corev1.Secret
	s.Eventually(func() bool {
		err = s.client.Get(ctx, types.NamespacedName{
			Namespace: managedProviderName.Namespace,
			Name:      "platform-mesh-provider-kubeconfig-my-managed-provider",
		}, &providerKubeconfigSecretInKind)
		return err == nil
	}, 240*time.Second, 5*time.Second, "waiting for provider kubeconfig secret locally in namespace e2e-managed-provider")

	providerKubeconfig := providerKubeconfigSecretInKcp.Data["kubeconfig"]
	s.NotEmpty(providerKubeconfig, "kubeconfig not set in provider kubeconfig secret")
	s.NotEmpty(providerKubeconfigSecretInKind.Data["kubeconfig"], "provider kubeconfig not copied to runtime cluster")
	s.Equal(providerKubeconfig, providerKubeconfigSecretInKind.Data["kubeconfig"], "kcp and kind provider kubeconfigs differ")

	// List ConfigMaps using the provider kubeconfig just to see if it works.

	providerClient, _, err := createKubernetesClient(providerKubeconfig, s.scheme)
	s.Require().NoError(err, "creating client from provider kubeconfig")

	s.logger.Info().Msgf("Listing ConfigMaps in provider workspace using provider's kubeconfig")
	cmList := &corev1.ConfigMapList{}
	err = providerClient.List(ctx, cmList, client.InNamespace("default"))
	s.NoError(err, "listing ConfigMaps in provider workspace using scoped kubeconfig should succeed")
	s.Greater(len(cmList.Items), 0,
		"listing ConfigMap in provider workspace using scoped kubeconfig should return non-zero items") // Should contain kube-root-ca.crt CM at least.

	// Check that ManagedProvider reaches Deployed phase and that the Deployment exists.

	s.Eventually(func() bool {
		err = s.client.Get(ctx, managedProviderName, &managedProvider)
		if err != nil {
			return false
		}
		return managedProvider.Status.Phase == "Deployed"
	}, 240*time.Second, 5*time.Second, "waiting for ManagedProvider to reach Phase=Deployed, but has err=%q Phase=%q", err, managedProvider.Status.Phase)
}
