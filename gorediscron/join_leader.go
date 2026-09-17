package gorediscron

import (
	"context"
	"errors"

	"github.com/robfig/cron/v3"
)

// JoinLeader starts one background goroutine that competes for the Redis leader lease.
// On win it runs leader work: in-process cron (enqueue ticks to Redis). On loss it stops cron
// and sleeps until the next election round (LeaseTTL/3). Idempotent.
func (s *Scheduler) JoinLeader(ctx context.Context) error {
	if s.leaderStarted.Load() {
		return nil
	}
	runCtx := s.ensureRunCtx(ctx)
	if !s.leaderStarted.CompareAndSwap(false, true) {
		return nil
	}

	s.leader.SetCallbacks(s.promoteLeaderCron, s.demoteLeaderCron)

	s.leaderWG.Add(1)
	go func() {
		defer s.leaderWG.Done()
		if err := s.leader.Run(runCtx); err != nil && !errors.Is(err, context.Canceled) {
			s.log.Error("join leader loop stopped", "err", err)
		}
	}()
	return nil
}

// startLeaderOnly runs election without tying cron to leadership (unusual; split tooling).
func (s *Scheduler) startLeaderOnly(ctx context.Context) error {
	if s.leaderStarted.Load() {
		return nil
	}
	runCtx := s.ensureRunCtx(ctx)
	if !s.leaderStarted.CompareAndSwap(false, true) {
		return nil
	}
	s.leader.SetCallbacks(nil, nil)
	s.leaderWG.Add(1)
	go func() {
		defer s.leaderWG.Done()
		if err := s.leader.Run(runCtx); err != nil && !errors.Is(err, context.Canceled) {
			s.log.Error("leader loop stopped", "err", err)
		}
	}()
	return nil
}

func (s *Scheduler) promoteLeaderCron() {
	s.mu.Lock()
	if s.cron == nil {
		s.cron = cron.New()
		s.mu.Unlock()
		if err := s.rebindAllCronJobs(); err != nil {
			s.log.Error("rebind cron jobs on promote", "err", err)
			return
		}
	} else {
		s.mu.Unlock()
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if s.cron == nil || s.cronStarted.Load() {
		return
	}
	s.cron.Start()
	s.cronStarted.Store(true)
	s.log.Info("cron started as leader", "instance", s.cfg.InstanceID)
}

func (s *Scheduler) demoteLeaderCron() {
	s.mu.Lock()
	c := s.cron
	s.mu.Unlock()
	if c == nil {
		return
	}

	stopCtx := c.Stop()
	<-stopCtx.Done()

	s.mu.Lock()
	s.cron = nil
	s.mu.Unlock()
	s.cronStarted.Store(false)
	s.registry.clearCronEntryIDs()
	s.log.Info("cron stopped after demotion", "instance", s.cfg.InstanceID)
}

func (s *Scheduler) rebindAllCronJobs() error {
	for _, job := range s.registry.activeCronJobs() {
		entryID, err := s.bindCron(job.name, job.cron)
		if err != nil {
			return err
		}
		s.registry.setEntryID(job.name, entryID)
	}
	return nil
}
