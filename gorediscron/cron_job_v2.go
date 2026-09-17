package gorediscron

import "time"

// CronJob registers a cron schedule that enqueues runs for an existing task.
type CronJob struct {
	// Name is the unique job id within the Redis namespace.
	Name string
	// TaskName must match a RegisterTask name on worker pods.
	TaskName string
	// Cron is a 5- or 6-field cron expression.
	Cron string
	// Args are JSON-serialized into each enqueued run.
	Args []any
	// Version is optional; auto-incremented when the job definition hash changes.
	Version int64
	// AllowParallel allows overlapping runs for this job.
	AllowParallel bool
	// Timeout bounds a single run. Zero uses DefaultRunTimeout.
	Timeout time.Duration
}

func (j CronJob) validate() error {
	if j.Name == "" {
		return errEmptyName
	}
	if j.TaskName == "" {
		return errEmptyTaskName
	}
	if j.Cron == "" {
		return errEmptyCron
	}
	return nil
}

func (j CronJob) runTimeout() time.Duration {
	if j.Timeout > 0 {
		return j.Timeout
	}
	return DefaultRunTimeout
}
