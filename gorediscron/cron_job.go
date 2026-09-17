package gorediscron

import "time"

// JobKind distinguishes repeat vs one-time schedulers (one-time is not implemented yet).
type JobKind string

const (
	JobKindRepeat  JobKind = "repeat"
	JobKindOneTime JobKind = "one_time"
)

// CronJobScheduler configures a repeat (cron) scheduler. Name is the stable identifier.
type CronJobScheduler struct {
	// Name uniquely identifies the scheduler (used as id for update, version, and stop).
	Name string
	// Cron expression (5- or 6-field).
	Cron string
	// Fn is executed by a worker when a tick is claimed.
	Fn JobFunc
	// Version is bumped on each Register when zero; set explicitly to force a generation.
	Version int64
	// AllowParallel allows overlapping runs when a tick fires before the previous run finishes.
	AllowParallel bool
	// Timeout bounds a single run. Zero uses DefaultRunTimeout.
	Timeout time.Duration
}

// DefaultRunTimeout is used when CronJobScheduler.Timeout is zero.
const DefaultRunTimeout = 30 * time.Minute

func (c CronJobScheduler) validate() error {
	if c.Name == "" {
		return errEmptyName
	}
	if c.Cron == "" {
		return errEmptyCron
	}
	if c.Fn == nil {
		return errNilFunc
	}
	return nil
}

func (c CronJobScheduler) runTimeout() time.Duration {
	if c.Timeout > 0 {
		return c.Timeout
	}
	return DefaultRunTimeout
}
