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
	cfg     Config
	rdb     redis.UniversalClient
	log     Logger
	metrics Metrics
	leader  *internal.Leader
	queue   *internal.RunQueue
	local   *internal.LocalQueue

	cron     *cron.Cron
	registry *schedulerRegistry

	runCtx    context.Context
	runCancel context.CancelFunc
	leaderWG  sync.WaitGroup
	workerWG  sync.WaitGroup
	claimWG   sync.WaitGroup

	workerMu       sync.Mutex
	workersStarted bool
	workerCount    int

	started atomic.Bool
	mu      sync.Mutex
}

// New builds a scheduler. Register schedulers, StartWorkers, then Start or StartWith.
func New(rdb redis.UniversalClient, cfg Config) (*Scheduler, error) {
	if rdb == nil {
		return nil, errors.New("gorediscron: redis client is required")
	}
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	m := metricsOrNop(cfg.Metrics)
	s := &Scheduler{
		cfg:      cfg,
		rdb:      rdb,
		log:      loggerOrNop(cfg.Logger),
		metrics:  m,
		leader:   internal.NewLeader(rdb, cfg.Namespace, cfg.InstanceID, cfg.LeaseTTL),
		registry: newSchedulerRegistry(),
	}
	s.queue = internal.NewRunQueue(rdb, cfg.Namespace, m.IncRedisOOM)
	s.local = internal.NewLocalQueue(cfg.queueCapacity(), m.SetLocalQueueDepth)
	return s, nil
}

// Jobs returns a snapshot of registered schedulers for APIs and the UI.
func (s *Scheduler) Jobs() []Job {
	return s.registry.snapshot(s.leader.IsLeader())
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
		s.claimWG.Wait()
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
