# Distributed cron runtime — design contract (v2)

This document is the target behavior for a **from-scratch** implementation. The current `gorediscron` code is a stepping stone; new work should converge here.

## 0. Three decoupled concerns

All three share one **Redis prefix** (`Config.Prefix` / namespace). Call order is **not** enforced.

| Concern | API (target) | Where state lives |
|---------|----------------|-------------------|
| **Tasks** | `RegisterTask(name, fn)` | In-process only (`fn(ctx, args...)` cannot be serialized) |
| **Jobs** | `RegisterJob(...)` (cron only for now) | Redis metadata + in-process cron binding on scheduler pods |
| **Workers** | `StartWorkerPool(ctx, cfg)` | In-process pool + **one claim loop per pod** |

Errors are raised only when something **runs**, not when something **registers**:

- `RegisterTask` / `RegisterJob` / `StartWorkerPool` return errors for invalid input or Redis failures.
- When a worker executes a claimed run: if the **task** is not registered on **this pod**, log an error (and metrics), then **ack** the run (do not spin forever). Typical causes: split deployment forgot `RegisterTask`, or job version/hash drift.

Scheduler pods enqueue **run records** into Redis; worker pods pull and execute using **local** task handlers.

---

## 1. Tasks

```go
type TaskFunc func(ctx context.Context, args ...any) error

func (r *Runtime) RegisterTask(taskName string, fn TaskFunc) error
```

- `taskName` is unique per process (duplicate → error or replace — pick **error** for safety).
- `args` are copied from job definition and/or tick payload (JSON in Redis queue).
- Tasks are **never** stored in Redis.

---

## 2. Jobs (cron only)

```go
type CronJob struct {
    Name     string   // unique job id within Prefix (operator-chosen)
    TaskName string   // must match a RegisterTask name on executors
    Cron     string   // 5- or 6-field expression
    Args     []any    // optional frozen args passed to TaskFunc
}

func (r *Runtime) RegisterJob(job CronJob) error
```

### Job definition hash

Deterministic id for “is this the same definition?”:

```text
jobID = "job_cronjob_" + sanitize(taskName) + "_" + sanitize(cron)
```

- `sanitize(s)`: lowercase, `[a-z0-9_]` only, collapse `_`, max length 64 (hash suffix if longer).
- Stored at Redis: `{prefix}:job:def:{job.Name}` → `{ jobID, taskName, cron, args, version }`.

### Idempotent re-register

| Case | Behavior |
|------|----------|
| Same `job.Name`, computed `jobID` unchanged | **No-op** (no version bump, no cron reschedule) |
| Same `job.Name`, `jobID` changed (cron or task changed) | **Overwrite** metadata, bump `version`, reschedule cron on this process |
| New `job.Name` | Insert |

`job.Name` is the **unique key** operators use; `jobID` detects definition equality.

---

## 3. Worker pool (same prefix)

```go
func (r *Runtime) StartWorkerPool(ctx context.Context, cfg WorkerPoolConfig) error
```

- `WorkerPoolConfig.NumberOfWorkers` ≥ 1.
- Uses the same `Prefix` as `RegisterJob` so enqueue and claim see one queue.
- **One Redis connection per pod** drives claiming: a dedicated `Conn` (or single goroutine owning one connection) runs the claim loop. Other goroutines use the shared client only for ack/active locks if needed, but **must not** compete on the blocking claim path.

### Claim → execute flow

```text
claim loop (1 conn, 1 goroutine per pod)
  → reserve local worker slot (semaphore)
  → if no slot: do NOT touch Redis pending (backoff)
  → if slot: atomic claim one run from Redis FIFO pending
  → dispatch to local bounded queue
worker goroutines
  → resolve job metadata (name/version)
  → lookup TaskName in local task registry
  → if missing: log error + metric; ack (skip)
  → else: fn(ctx, args...)
  → ack / release processing lease
```

---

## 4. FIFO queue and the “busy pod” problem

**Problem:** Runs sit in Redis. Pod A has all workers busy; Pod B has free workers. Work must not stick behind Pod A’s in-memory backlog while B is idle.

**Solution: global FIFO in Redis + reserve-then-claim**

```text
                    ┌─────────────────────────────────────┐
  cron leader       │  Redis LIST pending (FIFO)          │
  RPUSH / LPUSH ──► │  [run1][run2][run3]...              │
                    └──────────────┬──────────────────────┘
                                   │
         ┌─────────────────────────┼─────────────────────────┐
         ▼                         ▼                         ▼
    Pod A claim loop           Pod B claim loop         Pod C ...
    (only if slot free)        (only if slot free)
         │                         │
         ▼                         ▼
    local queue (bounded)      local queue
         │                         │
    worker goroutines          worker goroutines
```

Rules:

1. **Pending queue is only in Redis** (LIST or Stream with consumer groups). Enqueue is FIFO (e.g. `LPUSH` + claim from tail via `RPOPLPUSH`, or `XADD` + `XREADGROUP`).
2. **A pod claims only when it has a free local worker slot** (reserve-then-claim). If all workers are busy, the claim loop **does not** pull from Redis.
3. Therefore a run stays in the **shared** pending list until **some** pod with capacity claims it — not pinned to a busy pod.
4. After claim, move payload to a **processing** structure with a **lease TTL**. If the pod dies mid-run, reaper returns the run to pending (at-least-once).
5. Local queue is a **small buffer** (≈ worker count), not a second backlog. If local enqueue fails after claim, **requeue to Redis** and release the slot.

| Failure | Task loss? |
|---------|------------|
| Local workers all busy | No — waits in Redis |
| Pod crash after claim | No — lease → reclaim |
| Redis OOM | Yes (best-effort; metric + log) |

This is the same pattern as §4 in the previous META (reserve-then-claim); v2 makes the **task/job split** and **jobID** rules explicit.

---

## 5. Cron leader (enqueue only)

Only the Redis **cron leader** (lease per prefix) evaluates schedules and enqueues runs:

```text
{prefix}:leader  SET NX PX
```

On tick: `Enqueue(Run{ JobName, Version, ScheduledAt, Args })` — no `Fn` in payload.

Non-leader pods do not enqueue. They may still run workers.

---

## 6. Unknown task / version mismatch

When handling a claimed run:

| Condition | Action |
|-----------|--------|
| Job metadata missing in Redis | Log error; ack (poison) or short retry — prefer **ack + metric** after N tries |
| `version` < registered version on pod | Log; mark skipped; ack |
| Task not in local registry | **Log error** (“unknown task … for job …”); ack |
| Task panics / returns error | Log; ack; optional retry policy later |

No fatal error in `Register*` paths for “worker not ready yet”.

---

## 7. Target public surface

```go
type Runtime struct { ... }

func New(rdb redis.UniversalClient, cfg Config) (*Runtime, error)

func (r *Runtime) RegisterTask(name string, fn TaskFunc) error
func (r *Runtime) RegisterJob(job CronJob) error

func (r *Runtime) StartWorkerPool(ctx context.Context, cfg WorkerPoolConfig) error
func (r *Runtime) JoinLeader(ctx context.Context) error // one goroutine: election; cron only while leader

func (r *Runtime) Stop(ctx context.Context) error
```

Legacy `Scheduler` APIs may wrap this during migration.

---

## 8. Implementation checklist (v2)

- [ ] `RegisterTask` registry (in-process)
- [ ] `RegisterJob` + `jobID` hash + Redis metadata (noop vs overwrite)
- [ ] Cron leader + enqueue run JSON (no fn)
- [ ] Worker pool + **single claim connection** per pod
- [ ] Reserve-then-claim + Redis FIFO pending/processing + lease reaper
- [ ] Unknown-task path (log + ack)
- [ ] Metrics: claim, ack, skip, unknown_task, requeue, redis_oom
