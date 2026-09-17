package gorediscron

import "context"

// StartMode selects optional loops for StartWith (convenience only; no ordering enforced).
type StartMode struct {
	LeaderElection bool
	Cron           bool
	Workers        bool
	// WorkerPool is required when Workers is true.
	WorkerPool WorkerPoolConfig
}

func FullStartMode(workers WorkerPoolConfig) StartMode {
	return StartMode{LeaderElection: true, Cron: true, Workers: true, WorkerPool: workers}
}

func SchedulerPodMode() StartMode {
	return StartMode{LeaderElection: true, Cron: true, Workers: false}
}

func WorkerPodMode(cfg WorkerPoolConfig) StartMode {
	return StartMode{LeaderElection: false, Cron: false, Workers: true, WorkerPool: cfg}
}

// Start runs leader election and cron. It does not start the worker pool; call StartWorkerPool separately.
func (s *Scheduler) Start(ctx context.Context) error {
	if err := s.StartLeaderElection(ctx); err != nil {
		return err
	}
	return s.StartCron(ctx)
}

// StartWith enables selected loops. Each piece is independent; Register is never required beforehand.
func (s *Scheduler) StartWith(ctx context.Context, mode StartMode) error {
	if mode.LeaderElection {
		if err := s.StartLeaderElection(ctx); err != nil {
			return err
		}
	}
	if mode.Cron {
		if err := s.StartCron(ctx); err != nil {
			return err
		}
	}
	if mode.Workers {
		if err := s.StartWorkerPool(ctx, mode.WorkerPool); err != nil {
			return err
		}
	}
	return nil
}
