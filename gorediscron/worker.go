package gorediscron

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/namtph/go-redis-cron/gorediscron/internal"
	"github.com/redis/go-redis/v9"
)

const workerPollInterval = 2 * time.Second

// StartWorkers configures how many goroutines dequeue and execute runs.
// Call before Start.
func (s *Scheduler) StartWorkers(n int) error {
	if n < 1 {
		return errors.New("gorediscron: worker count must be at least 1")
	}
	s.workerMu.Lock()
	defer s.workerMu.Unlock()
	if s.workersStarted {
		return errors.New("gorediscron: workers already started")
	}
	s.workerCount = n
	s.workersStarted = true
	return nil
}

func (s *Scheduler) runWorkers(ctx context.Context) {
	s.workerMu.Lock()
	n := s.workerCount
	s.workerMu.Unlock()

	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			s.workerLoop(ctx)
		}()
	}
	wg.Wait()
}

func (s *Scheduler) workerLoop(ctx context.Context) {
	for {
		if ctx.Err() != nil {
			return
		}
		task, err := s.queue.Dequeue(ctx, workerPollInterval)
		if err != nil {
			if errors.Is(err, context.Canceled) {
				return
			}
			if errors.Is(err, redis.Nil) {
				continue
			}
			s.log.Error("dequeue failed", "err", err)
			continue
		}
		s.handleRun(ctx, task)
	}
}

func (s *Scheduler) handleRun(parent context.Context, task internal.RunTask) {
	rec, ok := s.registry.get(task.Name)
	if !ok || rec.stopped {
		s.registry.markRun(task.Name, RunStatusSkipped, nil)
		return
	}
	if task.Version < rec.version {
		s.registry.markRun(task.Name, RunStatusSkipped, nil)
		return
	}

	claimed, err := s.queue.TryClaimRun(parent, s.cfg.Namespace, s.cfg.InstanceID, task.Name, task.ScheduledAt)
	if err != nil {
		s.log.Error("claim run failed", "name", task.Name, "err", err)
		return
	}
	if !claimed {
		s.registry.markRun(task.Name, RunStatusSkipped, nil)
		return
	}

	releaseActive := func() {}
	if !rec.allowParallel {
		begin, err := s.queue.TryBeginActive(parent, s.cfg.Namespace, s.cfg.InstanceID, task.Name)
		if err != nil {
			s.log.Error("active lock failed", "name", task.Name, "err", err)
			return
		}
		if !begin {
			s.registry.markRun(task.Name, RunStatusSkipped, nil)
			return
		}
		releaseActive = func() {
			_ = s.queue.EndActive(context.Background(), s.cfg.Namespace, s.cfg.InstanceID, task.Name)
		}
	}

	runCtx, runCancel := context.WithTimeout(parent, rec.timeout)
	rec.setRunCancel(runCancel)

	runErr := rec.fn(runCtx)
	runCancel()
	rec.clearRunCancel()
	releaseActive()

	if errors.Is(runErr, context.Canceled) {
		s.registry.markRun(task.Name, RunStatusSkipped, runErr)
		return
	}
	if runErr != nil {
		s.registry.markRun(task.Name, RunStatusFailed, runErr)
		s.log.Error("job failed", "name", task.Name, "err", runErr)
		return
	}
	s.registry.markRun(task.Name, RunStatusSuccess, nil)
}
