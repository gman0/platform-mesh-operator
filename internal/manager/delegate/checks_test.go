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
	"net/http"
	"testing"
	"time"

	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/manager"

	"github.com/platform-mesh/platform-mesh-operator/internal/manager/delegate"
)

// syncableCache is a minimal cache.Cache whose WaitForCacheSync is controllable.
type syncableCache struct {
	cache.Cache // nil embedded
	synced      chan struct{}
}

func (c *syncableCache) WaitForCacheSync(ctx context.Context) bool {
	select {
	case <-c.synced:
		return true
	case <-ctx.Done():
		return false
	}
}

func TestCacheSyncChecker_Synced(t *testing.T) {
	c := &syncableCache{synced: make(chan struct{})}
	close(c.synced)

	checker := delegate.CacheSyncChecker(c)
	req, _ := http.NewRequest(http.MethodGet, "/", nil)
	if err := checker(req); err != nil {
		t.Fatalf("expected nil for synced cache, got %v", err)
	}
}

func TestCacheSyncChecker_NotSynced(t *testing.T) {
	c := &syncableCache{synced: make(chan struct{})} // never closed

	checker := delegate.CacheSyncChecker(c)

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, "/", nil)

	if err := checker(req); err == nil {
		t.Fatal("expected error for unsynced cache")
	}
}

func TestElectedChecker_Elected(t *testing.T) {
	elected := make(chan struct{})
	close(elected)

	checker := delegate.ElectedChecker(elected)
	req, _ := http.NewRequest(http.MethodGet, "/", nil)
	if err := checker(req); err != nil {
		t.Fatalf("expected nil for elected channel, got %v", err)
	}
}

func TestElectedChecker_NotElected(t *testing.T) {
	elected := make(chan struct{}) // never closed

	checker := delegate.ElectedChecker(elected)
	req, _ := http.NewRequest(http.MethodGet, "/", nil)
	if err := checker(req); err == nil {
		t.Fatal("expected error for non-elected channel")
	}
}

func TestCopyLeaderElectionOptions(t *testing.T) {
	leaseDur := 30 * time.Second
	renewDead := 15 * time.Second
	retryPer := 3 * time.Second

	src := manager.Options{
		LeaderElection:                true,
		LeaderElectionID:              "should-not-copy",
		LeaderElectionNamespace:       "should-not-copy",
		LeaderElectionResourceLock:    "should-not-copy",
		LeaseDuration:                 ptr.To(leaseDur),
		RenewDeadline:                 ptr.To(renewDead),
		RetryPeriod:                   ptr.To(retryPer),
		LeaderElectionReleaseOnCancel: true,
	}

	var dst manager.Options
	delegate.CopyLeaderElectionOptions(&dst, src)

	if dst.LeaderElection != src.LeaderElection {
		t.Error("LeaderElection not copied")
	}
	if dst.LeaseDuration == nil || *dst.LeaseDuration != leaseDur {
		t.Errorf("LeaseDuration = %v, want %v", dst.LeaseDuration, leaseDur)
	}
	if dst.RenewDeadline == nil || *dst.RenewDeadline != renewDead {
		t.Errorf("RenewDeadline = %v, want %v", dst.RenewDeadline, renewDead)
	}
	if dst.RetryPeriod == nil || *dst.RetryPeriod != retryPer {
		t.Errorf("RetryPeriod = %v, want %v", dst.RetryPeriod, retryPer)
	}
	if !dst.LeaderElectionReleaseOnCancel {
		t.Error("LeaderElectionReleaseOnCancel not copied")
	}

	// Fields that must NOT be copied when a proxy lock is used.
	if dst.LeaderElectionID != "" {
		t.Errorf("LeaderElectionID must not be copied, got %q", dst.LeaderElectionID)
	}
	if dst.LeaderElectionNamespace != "" {
		t.Errorf("LeaderElectionNamespace must not be copied, got %q", dst.LeaderElectionNamespace)
	}
	if dst.LeaderElectionResourceLock != "" {
		t.Errorf("LeaderElectionResourceLock must not be copied, got %q", dst.LeaderElectionResourceLock)
	}
}
