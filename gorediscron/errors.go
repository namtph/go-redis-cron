package gorediscron

import "errors"

var (
	errEmptyName = errors.New("gorediscron: scheduler name is required")
	errEmptyCron = errors.New("gorediscron: cron expression is required")
	errNilFunc   = errors.New("gorediscron: job func is required")
)
