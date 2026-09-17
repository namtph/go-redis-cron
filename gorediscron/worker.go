package gorediscron

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/namtph/go-redis-cron/gorediscron/internal"
)

const reaperInterval = 5 * time.Second

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

func (s *Scheduler) runClaimLoop(ctx context.Context) {
	backoff := s.cfg.claimBackoff()
	for {
		if ctx.Err() != nil {
			return
		}
		if !s.local.TryReserve() {
			s.sleep(ctx, backoff)
			continue
		}
		task, ok, err := s.queue.ClaimPending(ctx, s.cfg.InstanceID)
		if err != nil {
			s.local.ReleaseSlot()
			s.log.Error("claim pending failed", "err", err)
			s.sleep(ctx, backoff)
			continue
		}
		if !ok {
			s.local.ReleaseSlot()
			s.sleep(ctx, backoff)
			continue
		}
		s.metrics.IncClaim()

		claimed, err := s.queue.TryClaimRun(ctx, s.cfg.Namespace, s.cfg.InstanceID, task.Name, task.ScheduledAt)
		if err != nil {
			s.local.ReleaseSlot()
			_ = s.requeueRun(ctx, task)
			s.sleep(ctx, backoff)
			continue
		}
		if !claimed {
			s.local.ReleaseSlot()
			_ = s.queue.Ack(ctx, task)
			continue
		}

		if err := s.local.Enqueue(task); err != nil {
			s.local.ReleaseSlot()
			_ = s.requeueRun(ctx, task)
			s.sleep(ctx, backoff)
			continue
		}
	}
}

func (s *Scheduler) runReaper(ctx context.Context) {
	ticker := time.NewTicker(reaperInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			n, err := s.queue.ReclaimExpired(ctx)
			if err != nil {
				s.log.Error("reclaim expired runs", "err", err)
				continue
			}
			if n > 0 {
				s.metrics.IncRequeue()
				s.log.Info("reclaimed expired runs", "count", n)
			}
		}
	}
}

func (s *Scheduler) requeueRun(ctx context.Context, task internal.RunTask) error {
	_ = s.queue.ReleaseClaim(ctx, s.cfg.Namespace, task.Name, task.ScheduledAt)
	if err := s.queue.Requeue(ctx, task); err != nil {
		s.log.Error("requeue run failed", "name", task.Name, "err", err)
		return err
	}
	s.metrics.IncRequeue()
	return nil
}

func (s *Scheduler) sleep(ctx context.Context, d time.Duration) {
	t := time.NewTimer(d)
	select {
	case <-ctx.Done():
		t.Stop()
	case <-t.C:
	}
}

func (s *Scheduler) workerLoop(ctx context.Context) {
	for {
		task, err := s.local.Dequeue(ctx)
		if err != nil {
			return
		}
		s.handleRun(ctx, task)
	}
}

func (s *Scheduler) handleRun(parent context.Context, task internal.RunTask) {
	ack := func() {
		if err := s.queue.Ack(parent, task); err != nil {
			s.log.Error("ack run failed", "name", task.Name, "err", err)
			return
		}
		s.metrics.IncAck()
	}

	rec, ok := s.registry.get(task.Name)
	if !ok || rec.stopped {
		s.registry.markRun(task.Name, RunStatusSkipped, nil)
		ack()
		return
	}
	if task.Version < rec.version {
		s.registry.markRun(task.Name, RunStatusSkipped, nil)
		ack()
		return
	}

	releaseActive := func() {}
	if !rec.allowParallel {
		begin, err := s.queue.TryBeginActive(parent, s.cfg.Namespace, s.cfg.InstanceID, task.Name)
		if err != nil {
			s.log.Error("active lock failed", "name", task.Name, "err", err)
			_ = s.requeueRun(parent, task)
			return
		}
		if !begin {
			s.registry.markRun(task.Name, RunStatusSkipped, nil)
			_ = s.requeueRun(parent, task)
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
		ack()
		return
	}
	if runErr != nil {
		s.registry.markRun(task.Name, RunStatusFailed, runErr)
		s.log.Error("job failed", "name", task.Name, "err", runErr)
		ack()
		return
	}
	s.registry.markRun(task.Name, RunStatusSuccess, nil)
	ack()
}
