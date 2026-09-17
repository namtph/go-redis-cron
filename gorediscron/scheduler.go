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

// Runtime coordinates tasks, cron jobs, leader election, and workers.
type Runtime struct {
	cfg     Config
	rdb     redis.UniversalClient
	claimCloser interface{ Close() error }
	log     Logger
	metrics Metrics
	leader  *internal.Leader
	queue   *internal.RunQueue
	local   *internal.LocalQueue
	tasks   *taskRegistry

	cron     *cron.Cron
	registry *schedulerRegistry

	runCtx    context.Context
	runCancel context.CancelFunc
	leaderWG  sync.WaitGroup
	workerWG  sync.WaitGroup
	claimWG   sync.WaitGroup

	workerMu          sync.Mutex
	workerCount       int
	workerPoolStarted atomic.Bool
	leaderStarted     atomic.Bool
	cronStarted       atomic.Bool

	mu sync.Mutex
}

// Scheduler is an alias for Runtime (legacy).
type Scheduler = Runtime

// New builds a Runtime. Call RegisterTask, RegisterJob, JoinLeader, StartWorkerPool in any order.
func New(rdb redis.UniversalClient, cfg Config) (*Runtime, error) {
	if rdb == nil {
		return nil, errors.New("gorediscron: redis client is required")
	}
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	m := metricsOrNop(cfg.Metrics)
	var claimConn *redis.Conn
	if c, ok := rdb.(*redis.Client); ok {
		claimConn = c.Conn()
	}
	s := &Runtime{
		cfg:         cfg,
		rdb:         rdb,
		claimCloser: claimConn,
		log:      loggerOrNop(cfg.Logger),
		metrics:  m,
		leader:   internal.NewLeader(rdb, cfg.Namespace, cfg.InstanceID, cfg.LeaseTTL),
		registry: newSchedulerRegistry(),
		tasks:    newTaskRegistry(),
	}
	s.queue = internal.NewRunQueue(claimConn, rdb, cfg.Namespace, m.IncRedisOOM)
	s.local = internal.NewLocalQueue(cfg.queueCapacity(), m.SetLocalQueueDepth)
	return s, nil
}

// Jobs returns a snapshot of registered schedulers for APIs and the UI.
func (r *Runtime) Jobs() []Job {
	return r.registry.snapshot(r.leader.IsLeader())
}

// Stop shuts down running loops started via Start* methods.
func (r *Runtime) Stop(ctx context.Context) error {
	r.mu.Lock()
	cancel := r.runCancel
	r.mu.Unlock()
	if cancel == nil {
		return nil
	}
	cancel()

	stopCtx := ctx
	if stopCtx == nil {
		stopCtx = context.Background()
	}

	if r.cron != nil {
		cronStop := r.cron.Stop()
		select {
		case <-cronStop.Done():
		case <-stopCtx.Done():
			return stopCtx.Err()
		}
	}

	done := make(chan struct{})
	go func() {
		r.leader.Stop()
		r.leaderWG.Wait()
		r.workerWG.Wait()
		r.claimWG.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-stopCtx.Done():
		return stopCtx.Err()
	}

	if r.claimCloser != nil {
		_ = r.claimCloser.Close()
	}

	r.leaderStarted.Store(false)
	r.cronStarted.Store(false)
	r.workerPoolStarted.Store(false)
	r.mu.Lock()
	r.runCtx = nil
	r.runCancel = nil
	r.mu.Unlock()
	r.log.Info("scheduler stopped", "instance", r.cfg.InstanceID)
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

func (r *Runtime) ensureCron() *cron.Cron {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.cron == nil {
		r.cron = cron.New()
	}
	return r.cron
}
