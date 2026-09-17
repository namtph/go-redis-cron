package gorediscron

import (
	"errors"
	"time"
)

// Config configures a distributed scheduler instance.
type Config struct {
	// Namespace isolates Redis keys when multiple services share one database.
	Namespace string
	// InstanceID uniquely identifies this replica (pod name, hostname, etc.).
	InstanceID string
	// LeaseTTL is how long the leader lock survives without renewal.
	LeaseTTL time.Duration
	// QueueCapacity bounds the in-process run queue per pod (META §4).
	QueueCapacity int
	// ClaimBackoff is the delay when the local queue is full or Redis has no tasks.
	ClaimBackoff time.Duration
	// Logger receives operational events. Optional; defaults to no-op.
	Logger Logger
	// Metrics is optional observability (claims, requeues, OOM, queue depth).
	Metrics Metrics
}

func (c Config) validate() error {
	if c.Namespace == "" {
		return errors.New("gorediscron: Namespace is required")
	}
	if c.InstanceID == "" {
		return errors.New("gorediscron: InstanceID is required")
	}
	if c.LeaseTTL <= 0 {
		return errors.New("gorediscron: LeaseTTL must be positive")
	}
	return nil
}

func (c Config) queueCapacity() int {
	if c.QueueCapacity <= 0 {
		return 32
	}
	return c.QueueCapacity
}

func (c Config) claimBackoff() time.Duration {
	if c.ClaimBackoff <= 0 {
		return 50 * time.Millisecond
	}
	return c.ClaimBackoff
}
