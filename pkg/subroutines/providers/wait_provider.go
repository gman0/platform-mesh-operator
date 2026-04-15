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

const WaitProviderSubroutineName = "WaitProviderSubroutine"

// WaitProviderSubroutine polls the Provider resource in the kcp workspace
// until status.phase == "Ready", indicating that SA, RBAC, and the kubeconfig
// Secret have been created by the Provider controller.
type WaitProviderSubroutine struct {
	client client.Client
}

func NewWaitProviderSubroutine(client client.Client) *WaitProviderSubroutine {
	return &WaitProviderSubroutine{client: client}
}

func (r *WaitProviderSubroutine) GetName() string {
	return WaitProviderSubroutineName
}

func (r *WaitProviderSubroutine) Process(_ context.Context, _ client.Object) (subroutines.Result, error) {
	// TODO: fetch Provider from kcp workspace and requeue until phase == "Ready"
	return subroutines.OK(), nil
}

func (r *WaitProviderSubroutine) Finalize(_ context.Context, _ client.Object) (subroutines.Result, error) {
	return subroutines.OK(), nil
}

func (r *WaitProviderSubroutine) Finalizers(_ client.Object) []string {
	return []string{}
}
