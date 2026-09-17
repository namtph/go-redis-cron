package gorediscron

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/namtph/go-redis-cron/gorediscron/internal"
	"github.com/redis/go-redis/v9"
	"github.com/robfig/cron/v3"
)

// Scheduler runs cron jobs when this instance is the Redis-elected leader.
type Scheduler struct {
	cfg    Config
	rdb    redis.UniversalClient
	log    Logger
	leader *internal.Leader

	cron   *cron.Cron
	registry *jobRegistry

	runCtx context.Context
	runCancel context.CancelFunc
	leaderWG sync.WaitGroup

	started atomic.Bool
	mu      sync.Mutex
}

// New builds a scheduler. Call AddFunc before Start.
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
		registry: newJobRegistry(),
	}, nil
}

// AddFunc registers a cron job. Only the leader executes fn on schedule.
func (s *Scheduler) AddFunc(id, name, spec string, fn JobFunc) error {
	if s.started.Load() {
		return errors.New("gorediscron: cannot add job after Start")
	}
	if id == "" {
		return errors.New("gorediscron: job id is required")
	}
	if fn == nil {
		return errors.New("gorediscron: job func is required")
	}
	sched, err := parseSpec(spec)
	if err != nil {
		return err
	}

	wrapped := func() {
		if !s.leader.IsLeader() {
			s.registry.markSkipped(id)
			return
		}
		ctx := s.runCtx
		if ctx == nil {
			ctx = context.Background()
		}
		runErr := fn(ctx)
		s.registry.markRun(id, runErr)
		if runErr != nil {
			s.log.Error("job failed", "id", id, "err", runErr)
		}
	}

	entryID, err := s.ensureCron().AddFunc(spec, wrapped)
	if err != nil {
		return err
	}

	next := sched.Next(time.Now())
	s.registry.add(id, name, spec, entryID, next)
	return nil
}

// Jobs returns a snapshot of registered jobs for APIs and the UI.
func (s *Scheduler) Jobs() []Job {
	return s.registry.snapshot(s.leader.IsLeader())
}

// Start begins leader election and the cron runner.
func (s *Scheduler) Start(ctx context.Context) error {
	if !s.started.CompareAndSwap(false, true) {
		return errors.New("gorediscron: already started")
	}
	if s.registry.len() == 0 {
		return errors.New("gorediscron: no jobs registered")
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
	s.log.Info("scheduler started", "namespace", s.cfg.Namespace, "instance", s.cfg.InstanceID)
	return nil
}

// Stop shuts down cron, releases leadership, and waits for the leader loop.
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

	cronStop := s.ensureCron().Stop()
	select {
	case <-cronStop.Done():
	case <-stopCtx.Done():
		return stopCtx.Err()
	}

	done := make(chan struct{})
	go func() {
		s.leader.Stop()
		s.leaderWG.Wait()
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
