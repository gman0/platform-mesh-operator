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
	"context"
	"encoding/json"
	"fmt"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/leaderelection/resourcelock"
)

// LockSecondaryWhenPrimaryElected returns a resourcelock.Interface whose acquire/renew behaviour is
// driven by the elected and lost channels of the primary.
// Pass as Options.LeaderElectionResourceLockInterface on secondary managers.
//
//   - elected: closed when primary wins election (e.g. primary.Elected())
//   - lost:    closed when primary loses election; secondary's OnStoppedLeading fires
//     within at most RenewDeadline after this channel is closed
func LockSecondaryWhenPrimaryElected(identity string, leaseDurationSeconds int, elected, lost <-chan struct{}) resourcelock.Interface {
	return &electedGateLock{
		identity: identity,
		elected:  elected,
		lost:     lost,

		leaseDurationSeconds: leaseDurationSeconds,
	}
}

type electedGateLock struct {
	identity string
	elected  <-chan struct{}
	lost     <-chan struct{}

	leaseDurationSeconds int
}

// isElected reports true when elected is closed and lost is not yet closed.
func (l *electedGateLock) isElected() bool {
	select {
	case <-l.lost:
		return false
	default:
	}
	select {
	case <-l.elected:
		return true
	default:
	}
	return false
}

// Get returns a synthetic owned LeaderElectionRecord when primary is elected,
// or a not-found error before election or after loss. Raw bytes are always nil
// because the proxy lock has no external source of truth; the LE loop adapts.
func (l *electedGateLock) Get(_ context.Context) (*resourcelock.LeaderElectionRecord, []byte, error) {
	identity := l.identity

	if !l.isElected() {
		// We are not the leader, so someone else is. It doesn't really matter who,
		// as long as the identity is different from ours.
		identity += "-winner"
	}
	now := metav1.Now()

	lre := &resourcelock.LeaderElectionRecord{
		HolderIdentity:       identity,
		LeaseDurationSeconds: l.leaseDurationSeconds,
		RenewTime:            now,
		AcquireTime:          now,
	}
	lreJsonBytes, err := json.Marshal(lre)
	if err != nil {
		return nil, nil, err
	}

	return lre, lreJsonBytes, nil
}

// Create succeeds once elected fires (idempotent), letting the LE loop transition
// to the leader state on the first attempt after election. Before election it
// returns an error so the loop retries at RetryPeriod intervals.
func (l *electedGateLock) Create(_ context.Context, _ resourcelock.LeaderElectionRecord) error {
	if l.isElected() {
		return nil
	}
	return fmt.Errorf("%s: primary has not yet won election", l.identity)
}

// Update succeeds while elected, and fails once lost fires, causing the secondary's
// renew loop to declare loss after RenewDeadline.
func (l *electedGateLock) Update(_ context.Context, ler resourcelock.LeaderElectionRecord) error {
	if l.isElected() {
		return nil
	}
	return fmt.Errorf("%s: primary is no longer elected", l.identity)
}

// RecordEvent is a no-op; events are owned by the primary's LE loop.
func (l *electedGateLock) RecordEvent(string) {}

// Identity returns the fixed identity string passed to ElectedGateLock.
func (l *electedGateLock) Identity() string { return l.identity }

// Describe returns a human-readable description used in LE log output.
func (l *electedGateLock) Describe() string {
	return fmt.Sprintf("secondary/%s", l.identity)
}
