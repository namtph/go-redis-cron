package gorediscron

import (
	"context"
	"errors"
)

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
// Idempotent. Does not require Register.
func (s *Scheduler) StartWorkerPool(ctx context.Context, cfg WorkerPoolConfig) error {
	if err := cfg.validate(); err != nil {
		return err
	}
	if s.workerPoolStarted.Load() {
		return nil
	}
	runCtx := s.ensureRunCtx(ctx)
	if !s.workerPoolStarted.CompareAndSwap(false, true) {
		return nil
	}
	n := cfg.NumberOfWorkerInstances
	s.workerMu.Lock()
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
