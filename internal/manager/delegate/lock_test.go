/*
Copyright 2025 The Kubernetes Authors.

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

package delegate_test

import (
	"context"
	"strings"
	"testing"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	rl "k8s.io/client-go/tools/leaderelection/resourcelock"

	"github.com/platform-mesh/platform-mesh-operator/internal/manager/delegate"
)

func TestElectedGateLock_IdentityAndDescribe(t *testing.T) {
	lock := delegate.LockSecondaryWhenPrimaryElected("my-id", make(chan struct{}), make(chan struct{}))

	if got := lock.Identity(); got != "my-id" {
		t.Fatalf("Identity = %q, want %q", got, "my-id")
	}
	if d := lock.Describe(); !strings.Contains(d, "my-id") {
		t.Fatalf("Describe = %q, expected it to contain identity", d)
	}
	lock.RecordEvent("some event") // must not panic
}

func TestElectedGateLock_BeforeElected(t *testing.T) {
	elected := make(chan struct{})
	lost := make(chan struct{})
	lock := delegate.LockSecondaryWhenPrimaryElected("id", elected, lost)

	t.Run("Get returns not-found", func(t *testing.T) {
		rec, raw, err := lock.Get(context.Background())
		if rec != nil || raw != nil {
			t.Fatal("expected nil record and raw bytes before election")
		}
		if !apierrors.IsNotFound(err) {
			t.Fatalf("expected not-found error, got %v", err)
		}
	})

	t.Run("Create returns error", func(t *testing.T) {
		if err := lock.Create(context.Background(), rl.LeaderElectionRecord{}); err == nil {
			t.Fatal("expected error from Create before election")
		}
	})

	t.Run("Update returns error", func(t *testing.T) {
		if err := lock.Update(context.Background(), rl.LeaderElectionRecord{}); err == nil {
			t.Fatal("expected error from Update before election")
		}
	})
}

func TestElectedGateLock_AfterElected(t *testing.T) {
	elected := make(chan struct{})
	lost := make(chan struct{})
	close(elected)
	lock := delegate.LockSecondaryWhenPrimaryElected("holder", elected, lost)

	t.Run("Get returns owned record with correct identity", func(t *testing.T) {
		rec, raw, err := lock.Get(context.Background())
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if raw != nil {
			t.Fatal("expected nil raw bytes from proxy lock")
		}
		if rec == nil {
			t.Fatal("expected non-nil record after election")
		}
		if rec.HolderIdentity != "holder" {
			t.Fatalf("HolderIdentity = %q, want %q", rec.HolderIdentity, "holder")
		}
	})

	t.Run("Create returns nil (idempotent)", func(t *testing.T) {
		if err := lock.Create(context.Background(), rl.LeaderElectionRecord{}); err != nil {
			t.Fatalf("expected nil from Create after election, got %v", err)
		}
	})

	t.Run("Update returns nil (renew succeeds)", func(t *testing.T) {
		if err := lock.Update(context.Background(), rl.LeaderElectionRecord{}); err != nil {
			t.Fatalf("expected nil from Update after election, got %v", err)
		}
	})
}

func TestElectedGateLock_AfterLost(t *testing.T) {
	elected := make(chan struct{})
	lost := make(chan struct{})
	close(elected)
	close(lost)
	lock := delegate.LockSecondaryWhenPrimaryElected("id", elected, lost)

	t.Run("Get returns not-found", func(t *testing.T) {
		_, _, err := lock.Get(context.Background())
		if !apierrors.IsNotFound(err) {
			t.Fatalf("expected not-found after loss, got %v", err)
		}
	})

	t.Run("Create returns error", func(t *testing.T) {
		if err := lock.Create(context.Background(), rl.LeaderElectionRecord{}); err == nil {
			t.Fatal("expected error from Create after loss")
		}
	})

	t.Run("Update returns error (triggers OnStoppedLeading after RenewDeadline)", func(t *testing.T) {
		if err := lock.Update(context.Background(), rl.LeaderElectionRecord{}); err == nil {
			t.Fatal("expected error from Update after loss")
		}
	})
}

// lost fires without elected ever having fired.
func TestElectedGateLock_LostWithoutElected(t *testing.T) {
	lost := make(chan struct{})
	close(lost)
	lock := delegate.LockSecondaryWhenPrimaryElected("id", make(chan struct{}), lost)

	_, _, err := lock.Get(context.Background())
	if !apierrors.IsNotFound(err) {
		t.Fatalf("expected not-found, got %v", err)
	}

	if err := lock.Update(context.Background(), rl.LeaderElectionRecord{}); err == nil {
		t.Fatal("expected Update to fail when lost without election")
	}
}
