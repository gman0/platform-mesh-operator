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

const ProviderResourceSubroutineName = "ProviderResourceSubroutine"

// ProviderResourceSubroutine creates a Provider resource inside the provider
// workspace, triggering the Provider controller to bootstrap SA, RBAC, and the
// kubeconfig Secret on the kcp side.
type ProviderResourceSubroutine struct {
	client client.Client
}

func NewProviderResourceSubroutine(client client.Client) *ProviderResourceSubroutine {
	return &ProviderResourceSubroutine{client: client}
}

func (r *ProviderResourceSubroutine) GetName() string {
	return ProviderResourceSubroutineName
}

func (r *ProviderResourceSubroutine) Process(_ context.Context, _ client.Object) (subroutines.Result, error) {
	// TODO: create Provider resource in the provider workspace
	return subroutines.OK(), nil
}

func (r *ProviderResourceSubroutine) Finalize(_ context.Context, _ client.Object) (subroutines.Result, error) {
	// TODO: delete Provider resource on teardown
	return subroutines.OK(), nil
}

func (r *ProviderResourceSubroutine) Finalizers(_ client.Object) []string {
	return []string{}
}
