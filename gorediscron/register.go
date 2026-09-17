package gorediscron

import (
	"context"
	"fmt"
	"time"

	"github.com/namtph/go-redis-cron/gorediscron/internal"
	"github.com/redis/go-redis/v9"
	"github.com/robfig/cron/v3"
)

// Register adds or updates a repeat cron scheduler by Name (idempotent Redis metadata).
// Safe to call before or after StartCron / StartWorkerPool, and any number of times.
// Re-registering the same Name cancels an in-flight run, replaces cron/func options, and bumps Version.
func (s *Scheduler) Register(def CronJobScheduler) error {
	if err := def.validate(); err != nil {
		return err
	}

	sched, err := parseSpec(def.Cron)
	if err != nil {
		return err
	}

	version := def.Version
	if version <= 0 {
		if existing, ok := s.registry.get(def.Name); ok {
			version = existing.version + 1
		} else {
			version = 1
		}
	}

	if oldID := s.registry.removeEntryID(def.Name); oldID != 0 && s.cron != nil {
		s.cron.Remove(oldID)
	}

	entryID, err := s.bindCron(def.Name, def.Cron)
	if err != nil {
		return err
	}

	next := sched.Next(time.Now())
	s.registry.upsert(def, version, entryID, next)

	if err := s.persistMeta(context.Background(), def.Name, def.Cron, version, def.AllowParallel, false); err != nil {
		return err
	}

	s.log.Info("scheduler registered", "name", def.Name, "version", version)
	return nil
}

// StopScheduler stops future ticks for name and cancels any in-flight run.
func (s *Scheduler) StopScheduler(ctx context.Context, name string) error {
	if name == "" {
		return errEmptyName
	}
	if !s.registry.stop(name) {
		return fmt.Errorf("gorediscron: scheduler %q not found", name)
	}
	if oldID := s.registry.removeEntryID(name); oldID != 0 && s.cron != nil {
		s.cron.Remove(oldID)
	}
	return s.persistMeta(ctx, name, "", 0, false, true)
}

// AddFunc registers a cron job using Name as id. Prefer Register.
func (s *Scheduler) AddFunc(id, displayName, spec string, fn JobFunc) error {
	name := id
	if name == "" {
		name = displayName
	}
	return s.Register(CronJobScheduler{
		Name: name,
		Cron: spec,
		Fn:   fn,
	})
}

func (s *Scheduler) bindCron(name, spec string) (cron.EntryID, error) {
	wrapped := func() {
		if !s.leader.IsLeader() {
			return
		}
		rec, ok := s.registry.get(name)
		if !ok || rec.stopped {
			return
		}
		scheduledAt := time.Now().UnixMilli()
		task := internal.RunTask{
			Name:        name,
			Version:     rec.version,
			ScheduledAt: scheduledAt,
		}
		ctx := s.runCtx
		if ctx == nil {
			ctx = context.Background()
		}
		if err := s.queue.Enqueue(ctx, task); err != nil {
			if internal.IsRedisOOM(err) {
				s.metrics.IncRedisOOM()
			}
			s.log.Error("enqueue run failed", "name", name, "err", err)
			return
		}
		if sched, err := parseSpec(rec.cron); err == nil {
			s.registry.setNextRun(name, sched.Next(time.Now()))
		}
	}
	return s.ensureCron().AddFunc(spec, wrapped)
}

func (s *Scheduler) persistMeta(ctx context.Context, name, cronExpr string, version int64, allowParallel, stopped bool) error {
	key := internal.SchedMetaKey(s.cfg.Namespace, name)
	if stopped {
		err := s.rdb.HSet(ctx, key, map[string]interface{}{
			"stopped": "1",
		}).Err()
		if internal.IsRedisOOM(err) {
			s.metrics.IncRedisOOM()
		}
		return err
	}
	script := redis.NewScript(`
local key = KEYS[1]
local newVer = tonumber(ARGV[1])
local cron = ARGV[2]
local parallel = ARGV[3]
local cur = redis.call('HGET', key, 'version')
if cur then
  cur = tonumber(cur)
  if cur > newVer then
    return 0
  end
end
redis.call('HSET', key, 'version', newVer, 'cron', cron, 'allow_parallel', parallel, 'stopped', '0')
return 1
`)
	res, err := script.Run(ctx, s.rdb, []string{key}, version, cronExpr, boolToInt(allowParallel)).Int64()
	if internal.IsRedisOOM(err) {
		s.metrics.IncRedisOOM()
		return err
	}
	if err != nil {
		return err
	}
	if res == 0 {
		s.metrics.IncRegisterSkipped()
	} else {
		s.metrics.IncRegisterApplied()
	}
	return nil
}

func boolToInt(v bool) int {
	if v {
		return 1
	}
	return 0
}
