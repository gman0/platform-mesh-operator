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

// Package delegate provides DelegatedManager, which wires multiple in-process
// managers together so they share health probes, metrics, webhook server, and
// leader election as a single operational unit.
package delegate

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"sync"

	"k8s.io/client-go/rest"

	"sigs.k8s.io/controller-runtime/pkg/cluster"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"

	mcmanager "sigs.k8s.io/multicluster-runtime/pkg/manager"
	"sigs.k8s.io/multicluster-runtime/pkg/multicluster"
)

// DelegatedManager owns a primary multicluster-runtime manager and zero or more
// secondary managers that delegate all shared infrastructure to the primary.
// Secondaries may be added before or after Start.
type DelegatedManager struct {
	primary     mcmanager.Manager
	primaryOpts mcmanager.Options
	newManager  func(*rest.Config, multicluster.Provider, mcmanager.Options, ...mcmanager.Option) (mcmanager.Manager, error)

	mu          sync.Mutex
	secondaries []delegatedSecondary
	lostCh      chan struct{} // closed when primary's LE machinery stops

	// populated by Start; guarded by mu for AddSecondary post-Start
	started bool
	gctx    context.Context
	cancel  context.CancelFunc
	wg      sync.WaitGroup
	errCh   chan error // capacity 1; first fatal error wins
}

type delegatedSecondary struct {
	mgr  mcmanager.Manager
	name string
}

// lostSentinel is a NeedLeaderElection runnable that closes its channel when
// the primary's internalCtx is cancelled (always before leaderElectionCancel
// fires). It implements mcmanager.Runnable so it can be registered directly
// via primary.Add; Engage is a no-op because the sentinel is cluster-agnostic.
type lostSentinel struct{ ch chan struct{} }

func (s lostSentinel) NeedLeaderElection() bool { return true }
func (s lostSentinel) Start(ctx context.Context) error {
	<-ctx.Done()
	close(s.ch)
	return nil
}
func (s lostSentinel) Engage(_ context.Context, _ multicluster.ClusterName, _ cluster.Cluster) error {
	return nil
}

// New wraps primary and its construction Options in a DelegatedManager.
// opts must be the same Options value that was passed to the primary's constructor;
// it provides the webhook server, scheme, timing, and graceful-shutdown values
// that AddSecondary copies to each secondary.
//
// New registers two things on primary before returning:
//  1. A lostSentinel (NeedLeaderElection runnable) that closes an internal lostCh
//     when primary's internalCtx is cancelled — i.e. on any shutdown.
//  2. An aggregate "secondaries" readyz check that iterates the live secondaries
//     slice under a lock — the only viable approach because AddReadyzCheck is
//     unavailable after primary.Start() (internal.go:210).
//
// New must be called before primary.Start().
func New(primary mcmanager.Manager, opts mcmanager.Options) (*DelegatedManager, error) {
	d := &DelegatedManager{
		primary:     primary,
		primaryOpts: opts,
		newManager:  mcmanager.New,
		lostCh:      make(chan struct{}),
	}

	if err := primary.Add(lostSentinel{ch: d.lostCh}); err != nil {
		return nil, fmt.Errorf("delegate.New: registering lost sentinel: %w", err)
	}

	if err := primary.AddReadyzCheck("secondaries", func(req *http.Request) error {
		d.mu.Lock()
		secondaries := d.secondaries
		d.mu.Unlock()

		var errs []error
		for _, s := range secondaries {
			if !s.mgr.GetLocalManager().GetCache().WaitForCacheSync(req.Context()) {
				errs = append(errs, fmt.Errorf("%s: cache not synced", s.name))
			}
			select {
			case <-s.mgr.Elected():
			default:
				errs = append(errs, fmt.Errorf("%s: not yet elected", s.name))
			}
		}
		return errors.Join(errs...)
	}); err != nil {
		return nil, fmt.Errorf("delegate.New: registering readyz check: %w", err)
	}

	return d, nil
}

// Primary returns the primary manager.
func (d *DelegatedManager) Primary() mcmanager.Manager { return d.primary }

// AddSecondary creates a new manager for cfg with all delegation overrides applied
// and registers it. May be called before or after Start.
//
// Fields overridden unconditionally:
//
//	HealthProbeBindAddress  → "0"  (probe server disabled)
//	Metrics.BindAddress     → "0"  (metrics server disabled)
//	PprofBindAddress        → "0"  (pprof server disabled)
//	GracefulShutdownTimeout → same as primary
//	Scheme                  → primary.GetLocalManager().GetScheme() if opts.Scheme is nil
//	WebhookServer           → same as primary if primaryOpts.WebhookServer != nil
//
// When primaryOpts.LeaderElection is true (the common case), also overrides:
//
//	LeaderElection                      → true
//	LeaderElectionResourceLockInterface → ElectedGateLock proxy
//	LeaseDuration / RenewDeadline /
//	  RetryPeriod / ReleaseOnCancel     → copied from primary opts
//
// When primaryOpts.LeaderElection is false, LeaderElection is forced to false on
// the secondary as well (no LE loop, no proxy lock).
//
// name must be unique across all secondaries; it is used as the LE lock identity
// and appears in readyz error messages.
func (d *DelegatedManager) AddSecondary(name string, cfg *rest.Config, provider multicluster.Provider, opts mcmanager.Options) (mcmanager.Manager, error) {
	opts = d.applyDelegationOverrides(opts, name)

	mgr, err := d.newManager(cfg, provider, opts)
	if err != nil {
		return nil, fmt.Errorf("AddSecondary %q: %w", name, err)
	}

	if err := d.registerSecondary(mgr, name); err != nil {
		return nil, err
	}
	return mgr, nil
}

// Start starts all registered managers and blocks until all have stopped.
// Secondaries added via AddSecondary after Start is called are launched into the
// same context automatically.
// Start should be called at most once.
func (d *DelegatedManager) Start(ctx context.Context) error {
	gctx, cancel := context.WithCancel(ctx)

	d.mu.Lock()
	d.gctx = gctx
	d.cancel = cancel
	d.errCh = make(chan error, 1)
	d.started = true
	secondariesCopy := make([]delegatedSecondary, len(d.secondaries)) // So that we can iterate on thread-local copy...
	copy(secondariesCopy, d.secondaries)
	d.mu.Unlock()

	// Guard goroutine: keeps wg counter > 0 until gctx is cancelled, preventing
	// wg.Wait() from returning while a concurrent AddSecondary may still call wg.Add.
	d.wg.Add(1)
	go func() {
		defer d.wg.Done()
		<-gctx.Done()
	}()

	for _, s := range secondariesCopy {
		d.wg.Add(1)
		go func() {
			defer d.wg.Done()
			if err := s.mgr.Start(gctx); err != nil && !errors.Is(err, context.Canceled) {
				cancel()
				select {
				case d.errCh <- err:
				default:
				}
			}
		}()
	}

	// Primary drives the shared context: its return cancels all secondaries.
	d.wg.Add(1)
	go func() {
		defer d.wg.Done()
		if err := d.primary.Start(gctx); err != nil {
			select {
			case d.errCh <- err:
			default:
			}
		}
		cancel()
	}()

	d.wg.Wait()

	select {
	case err := <-d.errCh:
		return err
	default:
		return nil
	}
}

// applyDelegationOverrides returns opts with all delegation fields overridden.
func (d *DelegatedManager) applyDelegationOverrides(opts mcmanager.Options, name string) mcmanager.Options {
	opts.HealthProbeBindAddress = "0"
	opts.Metrics = metricsserver.Options{BindAddress: "0"}
	opts.PprofBindAddress = "0"
	opts.GracefulShutdownTimeout = d.primaryOpts.GracefulShutdownTimeout
	if opts.Scheme == nil {
		if d.primaryOpts.Scheme != nil {
			opts.Scheme = d.primaryOpts.Scheme
		} else {
			opts.Scheme = d.primary.GetLocalManager().GetScheme()
		}
	}
	if d.primaryOpts.WebhookServer != nil {
		opts.WebhookServer = d.primaryOpts.WebhookServer
	}
	if d.primaryOpts.LeaderElection {
		CopyLeaderElectionOptions(&opts, d.primaryOpts)
		opts.LeaderElectionResourceLockInterface = ElectedGateLock(
			name, d.primary.Elected(), d.lostCh,
		)
	} else {
		opts.LeaderElection = false
	}
	return opts
}

// registerSecondary appends mgr to the secondaries list and, if Start has already
// been called, launches mgr immediately into the running context.
func (d *DelegatedManager) registerSecondary(mgr mcmanager.Manager, name string) error {
	s := delegatedSecondary{mgr: mgr, name: name}

	d.mu.Lock()
	defer d.mu.Unlock()

	// Post-Start path: check that the group is not already stopping before
	// appending. If we returned an error after the append, the ghost entry would
	// remain in d.secondaries and the aggregate readyz check would report it as
	// unready indefinitely.
	if d.started {
		select {
		case <-d.gctx.Done():
			return fmt.Errorf("AddSecondary %q: manager group is stopping", name)
		default:
		}
	}

	d.secondaries = append(d.secondaries, s)

	if !d.started {
		return nil
	}

	d.wg.Add(1)
	go func() {
		defer d.wg.Done()
		if err := mgr.Start(d.gctx); err != nil && !errors.Is(err, context.Canceled) {
			d.cancel()
			select {
			case d.errCh <- err:
			default:
			}
		}
	}()

	return nil
}
