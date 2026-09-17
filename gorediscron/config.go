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
	// Logger receives operational events. Optional; defaults to no-op.
	Logger Logger
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
