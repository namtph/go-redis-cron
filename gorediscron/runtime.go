package gorediscron

import (
	"context"
	"errors"
)

// StartLeaderElection is deprecated; use JoinLeader, which also runs cron only while leader.
func (s *Scheduler) StartLeaderElection(ctx context.Context) error {
	return s.JoinLeader(ctx)
}

// StartCron starts the in-process cron on every pod and gates enqueue with IsLeader().
// Prefer JoinLeader for homogeneous deployments (cron runs only on the elected leader).
func (s *Scheduler) StartCron(ctx context.Context) error {
	if s.leaderStarted.Load() {
		return nil
	}
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
