package internal

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

// RunTask is enqueued when a cron schedule fires on the leader.
type RunTask struct {
	Name        string `json:"name"`
	Version     int64  `json:"version"`
	ScheduledAt int64  `json:"scheduledAt"` // UnixMilli
}

// RunQueue manages pending/processing runs in Redis.
type RunQueue struct {
	rdb       redis.UniversalClient
	namespace string
	pending   string
	processing string
	meta      string
	leases    string
	claimTTL  time.Duration
	activeTTL time.Duration
	onOOM     func()
}

func NewRunQueue(rdb redis.UniversalClient, namespace string, onOOM func()) *RunQueue {
	return &RunQueue{
		rdb:        rdb,
		namespace:  namespace,
		pending:    RunQueueKey(namespace),
		processing: RunProcessingListKey(namespace),
		meta:       RunProcessingMetaKey(namespace),
		leases:     RunLeaseKey(namespace),
		claimTTL:   10 * time.Minute,
		activeTTL:  30 * time.Minute,
		onOOM:      onOOM,
	}
}

func (q *RunQueue) Enqueue(ctx context.Context, task RunTask) error {
	b, err := json.Marshal(task)
	if err != nil {
		return err
	}
	err = q.rdb.LPush(ctx, q.pending, string(b)).Err()
	if IsRedisOOM(err) {
		q.onOOM()
	}
	if err != nil {
		return fmt.Errorf("enqueue run: %w", err)
	}
	return nil
}

// ClaimPending atomically moves one task from pending to processing and records a lease.
func (q *RunQueue) ClaimPending(ctx context.Context, workerID string) (RunTask, bool, error) {
	script := redis.NewScript(`
local pending = KEYS[1]
local processing = KEYS[2]
local meta = KEYS[3]
local leases = KEYS[4]
local worker = ARGV[1]
local leaseMs = tonumber(ARGV[2])
local raw = redis.call('RPOPLPUSH', pending, processing)
if not raw then
  return nil
end
local t = redis.call('TIME')
local sec = tonumber(t[1])
local usec = tonumber(t[2])
local deadlineMs = sec * 1000 + math.floor(usec / 1000) + leaseMs
redis.call('HSET', meta, raw, worker .. '|' .. tostring(deadlineMs))
redis.call('ZADD', leases, deadlineMs, raw)
return raw
`)
	raw, err := script.Run(ctx, q.rdb, []string{q.pending, q.processing, q.meta, q.leases},
		workerID, q.claimTTL.Milliseconds()).Text()
	if errors.Is(err, redis.Nil) || raw == "" {
		return RunTask{}, false, nil
	}
	if IsRedisOOM(err) {
		q.onOOM()
	}
	if err != nil {
		return RunTask{}, false, err
	}
	var task RunTask
	if err := json.Unmarshal([]byte(raw), &task); err != nil {
		return RunTask{}, false, err
	}
	return task, true, nil
}

// Requeue returns a processing task to the pending list.
func (q *RunQueue) Requeue(ctx context.Context, task RunTask) error {
	raw, err := json.Marshal(task)
	if err != nil {
		return err
	}
	script := redis.NewScript(`
local pending = KEYS[1]
local processing = KEYS[2]
local meta = KEYS[3]
local leases = KEYS[4]
local raw = ARGV[1]
redis.call('LREM', processing, 1, raw)
redis.call('HDEL', meta, raw)
redis.call('ZREM', leases, raw)
redis.call('LPUSH', pending, raw)
return 1
`)
	err = script.Run(ctx, q.rdb, []string{q.pending, q.processing, q.meta, q.leases}, string(raw)).Err()
	if IsRedisOOM(err) {
		q.onOOM()
	}
	return err
}

// Ack removes a task from processing after successful handling.
func (q *RunQueue) Ack(ctx context.Context, task RunTask) error {
	raw, err := json.Marshal(task)
	if err != nil {
		return err
	}
	script := redis.NewScript(`
local processing = KEYS[1]
local meta = KEYS[2]
local leases = KEYS[3]
local raw = ARGV[1]
redis.call('LREM', processing, 1, raw)
redis.call('HDEL', meta, raw)
redis.call('ZREM', leases, raw)
return 1
`)
	err = script.Run(ctx, q.rdb, []string{q.processing, q.meta, q.leases}, string(raw)).Err()
	if IsRedisOOM(err) {
		q.onOOM()
	}
	return err
}

// ReclaimExpired moves expired processing tasks back to pending.
func (q *RunQueue) ReclaimExpired(ctx context.Context) (int, error) {
	nowMs := time.Now().UnixMilli()
	script := redis.NewScript(`
local pending = KEYS[1]
local processing = KEYS[2]
local meta = KEYS[3]
local leases = KEYS[4]
local nowMs = tonumber(ARGV[1])
local expired = redis.call('ZRANGEBYSCORE', leases, '-inf', nowMs)
local count = 0
for _, raw in ipairs(expired) do
  redis.call('LREM', processing, 1, raw)
  redis.call('HDEL', meta, raw)
  redis.call('ZREM', leases, raw)
  redis.call('LPUSH', pending, raw)
  count = count + 1
end
return count
`)
	n, err := script.Run(ctx, q.rdb, []string{q.pending, q.processing, q.meta, q.leases}, nowMs).Int64()
	if IsRedisOOM(err) {
		q.onOOM()
	}
	return int(n), err
}

// ReleaseClaim removes the cross-pod dedup key so a requeued run can execute again.
func (q *RunQueue) ReleaseClaim(ctx context.Context, namespace, name string, scheduledAt int64) error {
	key := RunClaimKey(namespace, name, scheduledAt)
	err := q.rdb.Del(ctx, key).Err()
	if IsRedisOOM(err) {
		q.onOOM()
	}
	return err
}

func (q *RunQueue) TryClaimRun(ctx context.Context, namespace, instanceID, name string, scheduledAt int64) (bool, error) {
	key := RunClaimKey(namespace, name, scheduledAt)
	ok, err := q.rdb.SetNX(ctx, key, instanceID, q.claimTTL).Result()
	if IsRedisOOM(err) {
		q.onOOM()
	}
	if err != nil {
		return false, err
	}
	return ok, nil
}

// TryBeginActive returns true when a non-parallel scheduler may start running.
func (q *RunQueue) TryBeginActive(ctx context.Context, namespace, instanceID, name string) (bool, error) {
	key := ActiveRunKey(namespace, name)
	ok, err := q.rdb.SetNX(ctx, key, instanceID, q.activeTTL).Result()
	if IsRedisOOM(err) {
		q.onOOM()
	}
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
	if IsRedisOOM(err) {
		q.onOOM()
	}
	return err
}

// IsRedisOOM reports Redis out-of-memory write failures.
func IsRedisOOM(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToUpper(err.Error())
	return strings.Contains(msg, "OOM") || strings.Contains(msg, "OUT OF MEMORY")
}
