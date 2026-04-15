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

	"github.com/platform-mesh/subroutines"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const KubeconfigCopySubroutineName = "KubeconfigCopySubroutine"

// KubeconfigCopySubroutine copies the kubeconfig Secret produced by the
// Provider controller from the kcp provider workspace into the runtime
// namespace, and records its name in status.kubeconfigSecretRef.
type KubeconfigCopySubroutine struct {
	client client.Client
}

func NewKubeconfigCopySubroutine(client client.Client) *KubeconfigCopySubroutine {
	return &KubeconfigCopySubroutine{client: client}
}

func (r *KubeconfigCopySubroutine) GetName() string {
	return KubeconfigCopySubroutineName
}

func (r *KubeconfigCopySubroutine) Process(_ context.Context, _ client.Object) (subroutines.Result, error) {
	// TODO: read kubeconfig Secret from kcp workspace and write it to the
	// runtime namespace; update status.kubeconfigSecretRef
	return subroutines.OK(), nil
}

func (r *KubeconfigCopySubroutine) Finalize(_ context.Context, _ client.Object) (subroutines.Result, error) {
	// TODO: delete the runtime kubeconfig Secret on teardown
	return subroutines.OK(), nil
}

func (r *KubeconfigCopySubroutine) Finalizers(_ client.Object) []string {
	return []string{}
}
