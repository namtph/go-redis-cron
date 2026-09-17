package gorediscron

import (
	"context"
	"time"
)

// JobFunc is invoked by a worker when a scheduled run is claimed.
type JobFunc func(ctx context.Context) error

// Job describes a registered scheduler for observability and the built-in UI.
type Job struct {
	ID            string
	Name          string
	Kind          JobKind
	Spec          string
	Version       int64
	AllowParallel bool
	Stopped       bool
	NextRun       time.Time
	LastRun       time.Time
	LastError     string
	LastStatus    RunStatus
	LeaderRun     bool
}

// RunStatus is the outcome of the most recent execution.
type RunStatus string

const (
	RunStatusUnknown RunStatus = "unknown"
	RunStatusSuccess RunStatus = "success"
	RunStatusFailed  RunStatus = "failed"
	RunStatusSkipped RunStatus = "skipped"
)
