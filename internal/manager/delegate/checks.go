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

package delegate

import (
	"fmt"
	"net/http"

	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/healthz"

	mcmanager "sigs.k8s.io/multicluster-runtime/pkg/manager"
)

// CacheSyncChecker returns a healthz.Checker that reports ready once c is synced.
// The check blocks until the request's context expires if the cache is not yet synced,
// matching the readyz semantics of the default cache-sync gate that controllerManager
// installs for its own cache.
func CacheSyncChecker(c cache.Cache) healthz.Checker {
	return func(req *http.Request) error {
		if c.WaitForCacheSync(req.Context()) {
			return nil
		}
		return fmt.Errorf("cache not synced")
	}
}

// ElectedChecker returns a healthz.Checker that reports ready once elected is closed.
func ElectedChecker(elected <-chan struct{}) healthz.Checker {
	return func(_ *http.Request) error {
		select {
		case <-elected:
			return nil
		default:
			return fmt.Errorf("not yet elected")
		}
	}
}

// CopyLeaderElectionOptions copies the leader-election timing fields from src to dst
// that remain relevant when LeaderElectionResourceLockInterface is used: sets
// LeaderElection=true, and copies LeaseDuration, RenewDeadline, RetryPeriod, and
// LeaderElectionReleaseOnCancel. ID/Namespace/ResourceLock fields are intentionally
// not copied — they are ignored by controller-runtime when a pre-built lock is provided.
func CopyLeaderElectionOptions(dst *mcmanager.Options, src mcmanager.Options) {
	dst.LeaderElection = src.LeaderElection
	dst.LeaseDuration = src.LeaseDuration
	dst.RenewDeadline = src.RenewDeadline
	dst.RetryPeriod = src.RetryPeriod
	dst.LeaderElectionReleaseOnCancel = src.LeaderElectionReleaseOnCancel
}
