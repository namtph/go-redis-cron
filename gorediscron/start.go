package gorediscron

import (
	"context"
	"errors"
)

// StartMode selects which runtime loops StartWith runs (see docs/META.md).
type StartMode struct {
	// LeaderElection runs Redis leader election (required for cron ticks).
	LeaderElection bool
	// Cron runs in-process cron that enqueues ticks when this pod is leader.
	Cron bool
	// Workers runs the in-pod claim loop and worker pool (requires StartWorkers).
	Workers bool
}

// FullStartMode is the recommended full pod: election + cron + workers.
func FullStartMode() StartMode {
	return StartMode{LeaderElection: true, Cron: true, Workers: true}
}

// SchedulerPodMode registers ticks only (no local workers).
func SchedulerPodMode() StartMode {
	return StartMode{LeaderElection: true, Cron: true, Workers: false}
}

// WorkerPodMode runs claim loop + workers without hosting cron.
func WorkerPodMode() StartMode {
	return StartMode{LeaderElection: false, Cron: false, Workers: true}
}

// Start is equivalent to StartWith(ctx, FullStartMode()) when workers were configured.
func (s *Scheduler) Start(ctx context.Context) error {
	mode := FullStartMode()
	if !s.workersStarted {
		mode.Workers = false
	}
	return s.StartWith(ctx, mode)
}

// StartWith starts selected loops. Register and StartWorkers are independent (see META §2).
func (s *Scheduler) StartWith(ctx context.Context, mode StartMode) error {
	if !s.started.CompareAndSwap(false, true) {
		return errors.New("gorediscron: already started")
	}
	if mode.Cron && s.registry.len() == 0 {
		s.started.Store(false)
		return errNoSchedules
	}
	if mode.Workers && !s.workersStarted {
		s.started.Store(false)
		return errNoWorkers
	}

	runCtx, cancel := context.WithCancel(ctx)
	s.runCtx = runCtx
	s.runCancel = cancel

	if mode.LeaderElection {
		s.leaderWG.Add(1)
		go func() {
			defer s.leaderWG.Done()
			if err := s.leader.Run(runCtx); err != nil && !errors.Is(err, context.Canceled) {
				s.log.Error("leader loop stopped", "err", err)
			}
		}()
	}

	if mode.Cron {
		s.ensureCron().Start()
	}

	if mode.Workers {
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
	}

	s.log.Info("scheduler started",
		"namespace", s.cfg.Namespace,
		"instance", s.cfg.InstanceID,
		"workers", s.workerCount,
		"cron", mode.Cron,
		"claim_loop", mode.Workers,
	)
	return nil
}
