package gorediscron

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/namtph/go-redis-cron/gorediscron/internal"
	"github.com/redis/go-redis/v9"
	"github.com/robfig/cron/v3"
)

// Scheduler coordinates cron ticks (leader), run queueing, and worker execution.
type Scheduler struct {
	cfg    Config
	rdb    redis.UniversalClient
	log    Logger
	leader *internal.Leader
	queue  *internal.RunQueue

	cron     *cron.Cron
	registry *schedulerRegistry

	runCtx    context.Context
	runCancel context.CancelFunc
	leaderWG  sync.WaitGroup
	workerWG  sync.WaitGroup

	workerMu         sync.Mutex
	workersStarted   bool
	workerCount      int

	started atomic.Bool
	mu      sync.Mutex
}

// New builds a scheduler. Register schedulers, StartWorkers, then Start.
func New(rdb redis.UniversalClient, cfg Config) (*Scheduler, error) {
	if rdb == nil {
		return nil, errors.New("gorediscron: redis client is required")
	}
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	return &Scheduler{
		cfg:      cfg,
		rdb:      rdb,
		log:      loggerOrNop(cfg.Logger),
		leader:   internal.NewLeader(rdb, cfg.Namespace, cfg.InstanceID, cfg.LeaseTTL),
		queue:    internal.NewRunQueue(rdb, cfg.Namespace),
		registry: newSchedulerRegistry(),
	}, nil
}

// Jobs returns a snapshot of registered schedulers for APIs and the UI.
func (s *Scheduler) Jobs() []Job {
	return s.registry.snapshot(s.leader.IsLeader())
}

// Start begins leader election, cron scheduling, and worker goroutines.
func (s *Scheduler) Start(ctx context.Context) error {
	if !s.started.CompareAndSwap(false, true) {
		return errors.New("gorediscron: already started")
	}
	if !s.workersStarted {
		return errNoWorkers
	}
	if s.registry.len() == 0 {
		return errNoSchedules
	}

	runCtx, cancel := context.WithCancel(ctx)
	s.runCtx = runCtx
	s.runCancel = cancel

	s.leaderWG.Add(1)
	go func() {
		defer s.leaderWG.Done()
		if err := s.leader.Run(runCtx); err != nil && !errors.Is(err, context.Canceled) {
			s.log.Error("leader loop stopped", "err", err)
		}
	}()

	s.ensureCron().Start()

	s.workerWG.Add(1)
	go func() {
		defer s.workerWG.Done()
		s.runWorkers(runCtx)
	}()

	s.log.Info("scheduler started", "namespace", s.cfg.Namespace, "instance", s.cfg.InstanceID, "workers", s.workerCount)
	return nil
}

// Stop shuts down workers, cron, and leader election.
func (s *Scheduler) Stop(ctx context.Context) error {
	if !s.started.Load() {
		return nil
	}

	if s.runCancel != nil {
		s.runCancel()
	}

	stopCtx := ctx
	if stopCtx == nil {
		stopCtx = context.Background()
	}

	if s.cron != nil {
		cronStop := s.cron.Stop()
		select {
		case <-cronStop.Done():
		case <-stopCtx.Done():
			return stopCtx.Err()
		}
	}

	done := make(chan struct{})
	go func() {
		s.leader.Stop()
		s.leaderWG.Wait()
		s.workerWG.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-stopCtx.Done():
		return stopCtx.Err()
	}

	s.started.Store(false)
	s.log.Info("scheduler stopped", "instance", s.cfg.InstanceID)
	return nil
}

func parseSpec(spec string) (cron.Schedule, error) {
	parsers := []cron.Parser{
		cron.NewParser(cron.Second | cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow | cron.Descriptor),
		cron.NewParser(cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow | cron.Descriptor),
	}
	for _, p := range parsers {
		sched, err := p.Parse(spec)
		if err == nil {
			return sched, nil
		}
	}
	return nil, fmt.Errorf("gorediscron: invalid cron spec %q", spec)
}

func (s *Scheduler) ensureCron() *cron.Cron {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.cron == nil {
		s.cron = cron.New()
	}
	return s.cron
}
