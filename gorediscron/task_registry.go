package gorediscron

import (
	"context"
	"sync"
)

// TaskFunc runs when a claimed job references this task name.
type TaskFunc func(ctx context.Context, args ...any) error

type taskRegistry struct {
	mu   sync.RWMutex
	byName map[string]TaskFunc
}

func newTaskRegistry() *taskRegistry {
	return &taskRegistry{byName: make(map[string]TaskFunc)}
}

func (t *taskRegistry) register(name string, fn TaskFunc) {
	t.mu.Lock()
	t.byName[name] = fn
	t.mu.Unlock()
}

func (t *taskRegistry) get(name string) (TaskFunc, bool) {
	t.mu.RLock()
	fn, ok := t.byName[name]
	t.mu.RUnlock()
	return fn, ok
}

// RegisterTask binds a handler by task name (in-process only). Re-registering the same name replaces fn.
func (r *Runtime) RegisterTask(taskName string, fn TaskFunc) error {
	if taskName == "" {
		return errEmptyTaskName
	}
	if fn == nil {
		return errNilFunc
	}
	r.tasks.register(taskName, fn)
	return nil
}
