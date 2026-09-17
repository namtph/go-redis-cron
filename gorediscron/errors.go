package gorediscron

import "errors"

var (
	errEmptyName               = errors.New("gorediscron: job name is required")
	errEmptyTaskName           = errors.New("gorediscron: task name is required")
	errEmptyCron               = errors.New("gorediscron: cron expression is required")
	errNilFunc                 = errors.New("gorediscron: job func is required")
	errInvalidWorkerPoolConfig = errors.New("gorediscron: NumberOfWorkerInstances must be at least 1")
)
