package gorediscron

import (
	"context"
	"errors"
)

// SetWorkerCount sets how many goroutines StartWorkerPool will run.
// Safe before the worker pool is started; may be called multiple times until then.
// Does not start any goroutines.
func (s *Scheduler) SetWorkerCount(n int) error {
	if n < 1 {
		return errors.New("gorediscron: worker count must be at least 1")
	}
	if s.workerPoolStarted.Load() {
		return errors.New("gorediscron: worker pool already running")
	}
	s.workerMu.Lock()
	defer s.workerMu.Unlock()
	s.workerCount = n
	s.workerCountSet = true
	return nil
}

// StartWorkers is an alias for SetWorkerCount (count only, does not start goroutines).
func (s *Scheduler) StartWorkers(n int) error {
	return s.SetWorkerCount(n)
}

// StartLeaderElection runs the Redis leader loop. Idempotent. Does not require Register.
func (s *Scheduler) StartLeaderElection(ctx context.Context) error {
	if s.leaderStarted.Load() {
		return nil
	}
	runCtx := s.ensureRunCtx(ctx)
	if !s.leaderStarted.CompareAndSwap(false, true) {
		return nil
	}
	s.leaderWG.Add(1)
	go func() {
		defer s.leaderWG.Done()
		if err := s.leader.Run(runCtx); err != nil && !errors.Is(err, context.Canceled) {
			s.log.Error("leader loop stopped", "err", err)
		}
	}()
	return nil
}

// StartCron starts the in-process cron scheduler. Idempotent. Does not require Register first;
// jobs added later via Register are picked up (if cron is already running, entries attach immediately).
func (s *Scheduler) StartCron(ctx context.Context) error {
	_ = s.ensureRunCtx(ctx)
	if s.cronStarted.Load() {
		return nil
	}
	if !s.cronStarted.CompareAndSwap(false, true) {
		return nil
	}
	s.ensureCron().Start()
	return nil
}

// StartWorkerPool starts the Redis claim loop, lease reaper, and worker goroutines.
// Idempotent. Does not require Register. Uses SetWorkerCount value, or 1 if unset.
func (s *Scheduler) StartWorkerPool(ctx context.Context) error {
	if s.workerPoolStarted.Load() {
		return nil
	}
	runCtx := s.ensureRunCtx(ctx)
	if !s.workerPoolStarted.CompareAndSwap(false, true) {
		return nil
	}
	s.workerMu.Lock()
	n := s.workerCount
	if n < 1 {
		n = 1
	}
	s.workerCount = n
	s.workerMu.Unlock()

	s.workerWG.Add(1)
	go func() {
		defer s.workerWG.Done()
		s.runWorkers(runCtx)
	}()
	s.claimWG.Add(1)
	go func() {
		defer s.claimWG.Done()
		s.runClaimLoop(runCtx)
	}()
	s.claimWG.Add(1)
	go func() {
		defer s.claimWG.Done()
		s.runReaper(runCtx)
	}()
	s.log.Info("worker pool started", "workers", n, "instance", s.cfg.InstanceID)
	return nil
}

func (s *Scheduler) ensureRunCtx(ctx context.Context) context.Context {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.runCtx == nil {
		s.runCtx, s.runCancel = context.WithCancel(ctx)
	}
	return s.runCtx
}
