package internal

import (
	"context"
	"errors"

	"golang.org/x/sync/semaphore"
)

// ErrLocalEnqueueFailed is returned when a reserved slot cannot accept a task.
var ErrLocalEnqueueFailed = errors.New("gorediscron: local enqueue failed after claim")

// LocalQueue is a bounded in-process queue with reserve-then-claim support.
type LocalQueue struct {
	capacity int
	tasks    chan RunTask
	slots    *semaphore.Weighted
	onDepth  func(int)
}

// NewLocalQueue creates a queue with the given capacity. onDepth is optional metrics hook.
func NewLocalQueue(capacity int, onDepth func(int)) *LocalQueue {
	if capacity <= 0 {
		capacity = 32
	}
	q := &LocalQueue{
		capacity: capacity,
		tasks:    make(chan RunTask, capacity),
		slots:    semaphore.NewWeighted(int64(capacity)),
		onDepth:  onDepth,
	}
	q.reportDepth()
	return q
}

// TryReserve acquires one queue slot without blocking.
func (q *LocalQueue) TryReserve() bool {
	return q.slots.TryAcquire(1)
}

// ReleaseSlot returns a reserved slot when claim or enqueue failed.
func (q *LocalQueue) ReleaseSlot() {
	q.slots.Release(1)
}

// Enqueue delivers a claimed task to workers.
func (q *LocalQueue) Enqueue(task RunTask) error {
	select {
	case q.tasks <- task:
		q.reportDepth()
		return nil
	default:
		return ErrLocalEnqueueFailed
	}
}

// Dequeue blocks until a task is available or ctx is cancelled.
func (q *LocalQueue) Dequeue(ctx context.Context) (RunTask, error) {
	select {
	case <-ctx.Done():
		return RunTask{}, ctx.Err()
	case task := <-q.tasks:
		q.slots.Release(1)
		q.reportDepth()
		return task, nil
	}
}

// Depth returns tasks waiting in the queue.
func (q *LocalQueue) Depth() int {
	return len(q.tasks)
}

func (q *LocalQueue) reportDepth() {
	if q.onDepth != nil {
		q.onDepth(len(q.tasks))
	}
}
