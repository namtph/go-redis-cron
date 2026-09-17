package internal

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

// RunTask is enqueued when a cron schedule fires on the leader.
type RunTask struct {
	Name        string `json:"name"`
	Version     int64  `json:"version"`
	ScheduledAt int64  `json:"scheduledAt"` // UnixMilli
}

// RunQueue pushes and pops scheduled runs.
type RunQueue struct {
	rdb       redis.UniversalClient
	queueKey  string
	claimTTL  time.Duration
	activeTTL time.Duration
	tag       string
}

func NewRunQueue(rdb redis.UniversalClient, namespace string) *RunQueue {
	return &RunQueue{
		rdb:       rdb,
		queueKey:  RunQueueKey(namespace),
		claimTTL:  10 * time.Minute,
		activeTTL: 30 * time.Minute,
		tag:       tag(namespace),
	}
}

func (q *RunQueue) Enqueue(ctx context.Context, task RunTask) error {
	b, err := json.Marshal(task)
	if err != nil {
		return err
	}
	if err := q.rdb.LPush(ctx, q.queueKey, b).Err(); err != nil {
		return fmt.Errorf("enqueue run: %w", err)
	}
	return nil
}

func (q *RunQueue) Dequeue(ctx context.Context, timeout time.Duration) (RunTask, error) {
	res, err := q.rdb.BRPop(ctx, timeout, q.queueKey).Result()
	if err != nil {
		return RunTask{}, err
	}
	if len(res) != 2 {
		return RunTask{}, fmt.Errorf("unexpected brpop result: %v", res)
	}
	var task RunTask
	if err := json.Unmarshal([]byte(res[1]), &task); err != nil {
		return RunTask{}, err
	}
	return task, nil
}

// TryClaimRun returns true if this instance owns the tick (cross-pod dedup).
func (q *RunQueue) TryClaimRun(ctx context.Context, namespace, instanceID, name string, scheduledAt int64) (bool, error) {
	key := RunClaimKey(namespace, name, scheduledAt)
	ok, err := q.rdb.SetNX(ctx, key, instanceID, q.claimTTL).Result()
	if err != nil {
		return false, err
	}
	return ok, nil
}

// TryBeginActive returns true when a non-parallel scheduler may start running.
func (q *RunQueue) TryBeginActive(ctx context.Context, namespace, instanceID, name string) (bool, error) {
	key := ActiveRunKey(namespace, name)
	ok, err := q.rdb.SetNX(ctx, key, instanceID, q.activeTTL).Result()
	if err != nil {
		return false, err
	}
	return ok, nil
}

// EndActive releases the in-flight marker for a scheduler name.
func (q *RunQueue) EndActive(ctx context.Context, namespace, instanceID, name string) error {
	key := ActiveRunKey(namespace, name)
	script := redis.NewScript(`
if redis.call("GET", KEYS[1]) == ARGV[1] then
  return redis.call("DEL", KEYS[1])
end
return 0
`)
	_, err := script.Run(ctx, q.rdb, []string{key}, instanceID).Result()
	return err
}
