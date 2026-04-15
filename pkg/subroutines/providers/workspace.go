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

const WorkspaceSubroutineName = "WorkspaceSubroutine"

// WorkspaceSubroutine creates the provider workspace in kcp under
// :root:providers:<name> (or spec.workspacePath if set).
type WorkspaceSubroutine struct {
	client client.Client
}

func NewWorkspaceSubroutine(client client.Client) *WorkspaceSubroutine {
	return &WorkspaceSubroutine{client: client}
}

func (r *WorkspaceSubroutine) GetName() string {
	return WorkspaceSubroutineName
}

func (r *WorkspaceSubroutine) Process(_ context.Context, _ client.Object) (subroutines.Result, error) {
	// TODO: create provider workspace in kcp with the correct WorkspaceType
	return subroutines.OK(), nil
}

func (r *WorkspaceSubroutine) Finalize(_ context.Context, _ client.Object) (subroutines.Result, error) {
	// TODO: delete workspace when spec.cleanupOnDelete is true
	return subroutines.OK(), nil
}

func (r *WorkspaceSubroutine) Finalizers(_ client.Object) []string {
	return []string{}
}
