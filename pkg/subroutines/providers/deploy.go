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

const DeploySubroutineName = "DeploySubroutine"

// DeploySubroutine resolves the OCM components referenced in spec.controller
// and spec.portal, extracts their Helm charts and image references, and
// deploys them to the target cluster. It also performs continuous drift
// detection on subsequent reconciliations.
type DeploySubroutine struct {
	client client.Client
}

func NewDeploySubroutine(client client.Client) *DeploySubroutine {
	return &DeploySubroutine{client: client}
}

func (r *DeploySubroutine) GetName() string {
	return DeploySubroutineName
}

func (r *DeploySubroutine) Process(_ context.Context, _ client.Object) (subroutines.Result, error) {
	// TODO: resolve OCM component → extract Helm chart + image ref
	// TODO: deploy controller Helm release
	// TODO: deploy portal Helm release (if spec.portal is set)
	return subroutines.OK(), nil
}

func (r *DeploySubroutine) Finalize(_ context.Context, _ client.Object) (subroutines.Result, error) {
	// TODO: uninstall Helm releases on teardown
	return subroutines.OK(), nil
}

func (r *DeploySubroutine) Finalizers(_ client.Object) []string {
	return []string{}
}
