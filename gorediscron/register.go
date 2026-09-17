package gorediscron

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	"github.com/namtph/go-redis-cron/gorediscron/internal"
	"github.com/redis/go-redis/v9"
	"github.com/robfig/cron/v3"
)

// RegisterJob adds or updates a cron job in Redis. Same definition hash (task + cron) is a no-op.
// A changed hash for the same job Name overwrites metadata and bumps version.
func (r *Runtime) RegisterJob(job CronJob) error {
	if err := job.validate(); err != nil {
		return err
	}

	sched, err := parseSpec(job.Cron)
	if err != nil {
		return err
	}

	jobID := CronJobID(job.TaskName, job.Cron)
	ctx := context.Background()

	if rec, ok := r.registry.get(job.Name); ok && rec.jobID == jobID && !rec.stopped {
		r.metrics.IncRegisterSkipped()
		return nil
	}

	key := internal.SchedMetaKey(r.cfg.Namespace, job.Name)
	storedID, err := r.rdb.HGet(ctx, key, "job_id").Result()
	if err == nil && storedID == jobID {
		if err := r.syncJobFromRedis(ctx, job, jobID, sched); err != nil {
			return err
		}
		r.metrics.IncRegisterSkipped()
		return nil
	}

	version := job.Version
	if version <= 0 {
		if existing, ok := r.registry.get(job.Name); ok {
			version = existing.version + 1
		} else if v, err := r.rdb.HGet(ctx, key, "version").Result(); err == nil && v != "" {
			if parsed, err := strconv.ParseInt(v, 10, 64); err == nil {
				version = parsed + 1
			} else {
				version = 1
			}
		} else {
			version = 1
		}
	}

	if oldID := r.registry.removeEntryID(job.Name); oldID != 0 && r.cron != nil {
		r.cron.Remove(oldID)
	}

	entryID, err := r.bindCron(job.Name, job.Cron)
	if err != nil {
		return err
	}

	next := sched.Next(time.Now())
	r.registry.upsertJob(job, jobID, version, entryID, next)

	if err := r.persistJobMeta(ctx, job, jobID, version, false); err != nil {
		return err
	}

	r.log.Info("job registered", "name", job.Name, "task", job.TaskName, "version", version, "job_id", jobID)
	return nil
}

func (r *Runtime) syncJobFromRedis(ctx context.Context, job CronJob, jobID string, sched cron.Schedule) error {
	key := internal.SchedMetaKey(r.cfg.Namespace, job.Name)
	version := int64(1)
	if v, err := r.rdb.HGet(ctx, key, "version").Result(); err == nil && v != "" {
		version, _ = strconv.ParseInt(v, 10, 64)
	}
	if rec, ok := r.registry.get(job.Name); ok && rec.jobID == jobID && rec.entryID != 0 {
		return nil
	}
	if oldID := r.registry.removeEntryID(job.Name); oldID != 0 && r.cron != nil {
		r.cron.Remove(oldID)
	}
	entryID, err := r.bindCron(job.Name, job.Cron)
	if err != nil {
		return err
	}
	r.registry.upsertJob(job, jobID, version, entryID, sched.Next(time.Now()))
	return nil
}

// Register adds a cron job and registers the task handler (legacy). Prefer RegisterTask + RegisterJob.
func (r *Runtime) Register(def CronJobScheduler) error {
	if err := def.validate(); err != nil {
		return err
	}
	if err := r.RegisterTask(def.Name, func(ctx context.Context, args ...any) error {
		return def.Fn(ctx)
	}); err != nil {
		return err
	}
	return r.RegisterJob(CronJob{
		Name:          def.Name,
		TaskName:      def.Name,
		Cron:          def.Cron,
		AllowParallel: def.AllowParallel,
		Timeout:       def.Timeout,
		Version:       def.Version,
	})
}

// StopScheduler stops future ticks for name and cancels any in-flight run.
func (r *Runtime) StopScheduler(ctx context.Context, name string) error {
	if name == "" {
		return errEmptyName
	}
	if !r.registry.stop(name) {
		return fmt.Errorf("gorediscron: scheduler %q not found", name)
	}
	if oldID := r.registry.removeEntryID(name); oldID != 0 && r.cron != nil {
		r.cron.Remove(oldID)
	}
	return r.persistJobMeta(ctx, CronJob{Name: name}, "", 0, true)
}

// AddFunc registers a cron job using Name as id. Prefer RegisterTask + RegisterJob.
func (r *Runtime) AddFunc(id, displayName, spec string, fn JobFunc) error {
	name := id
	if name == "" {
		name = displayName
	}
	return r.Register(CronJobScheduler{
		Name: name,
		Cron: spec,
		Fn:   fn,
	})
}

func (r *Runtime) bindCron(name, spec string) (cron.EntryID, error) {
	wrapped := func() {
		if r.leaderStarted.Load() && !r.leader.IsLeader() {
			return
		}
		rec, ok := r.registry.get(name)
		if !ok || rec.stopped {
			return
		}
		scheduledAt := time.Now().UnixMilli()
		task := internal.RunTask{
			Name:        name,
			TaskName:    rec.taskName,
			Version:     rec.version,
			ScheduledAt: scheduledAt,
			Args:        rec.args,
		}
		ctx := r.runCtx
		if ctx == nil {
			ctx = context.Background()
		}
		if err := r.queue.Enqueue(ctx, task); err != nil {
			if internal.IsRedisOOM(err) {
				r.metrics.IncRedisOOM()
			}
			r.log.Error("enqueue run failed", "name", name, "err", err)
			return
		}
		if sched, err := parseSpec(rec.cron); err == nil {
			r.registry.setNextRun(name, sched.Next(time.Now()))
		}
	}
	sched, err := parseSpec(spec)
	if err != nil {
		return 0, err
	}
	return r.ensureCron().Schedule(sched, cron.FuncJob(wrapped)), nil
}

func (r *Runtime) persistJobMeta(ctx context.Context, job CronJob, jobID string, version int64, stopped bool) error {
	key := internal.SchedMetaKey(r.cfg.Namespace, job.Name)
	if stopped {
		err := r.rdb.HSet(ctx, key, map[string]interface{}{
			"stopped": "1",
		}).Err()
		if internal.IsRedisOOM(err) {
			r.metrics.IncRedisOOM()
		}
		return err
	}

	argsJSON, err := json.Marshal(job.Args)
	if err != nil {
		return err
	}

	script := redis.NewScript(`
local key = KEYS[1]
local newVer = tonumber(ARGV[1])
local cron = ARGV[2]
local parallel = ARGV[3]
local jobID = ARGV[4]
local taskName = ARGV[5]
local args = ARGV[6]
local cur = redis.call('HGET', key, 'version')
if cur then
  cur = tonumber(cur)
  if cur > newVer then
    return 0
  end
end
redis.call('HSET', key,
  'version', newVer,
  'cron', cron,
  'allow_parallel', parallel,
  'stopped', '0',
  'job_id', jobID,
  'task_name', taskName,
  'args', args)
return 1
`)
	res, err := script.Run(ctx, r.rdb, []string{key},
		version, job.Cron, boolToInt(job.AllowParallel), jobID, job.TaskName, string(argsJSON)).Int64()
	if internal.IsRedisOOM(err) {
		r.metrics.IncRedisOOM()
		return err
	}
	if err != nil {
		return err
	}
	if res == 0 {
		r.metrics.IncRegisterSkipped()
	} else {
		r.metrics.IncRegisterApplied()
	}
	return nil
}

func boolToInt(v bool) int {
	if v {
		return 1
	}
	return 0
}
