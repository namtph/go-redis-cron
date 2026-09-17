# Distributed job runtime — meta (sections 1–4)

This document is the product contract for `gorediscron`. API mapping:

| META | gorediscron |
|------|-------------|
| `registerJobs` | `Register(CronJobScheduler)` |
| `startWorkerPool` | `StartWorkers(n)` + worker goroutines via `StartWith` |
| `startLeader` (claim loop) | `StartWith` with `Workers: true` (claim loop + reaper) |
| Cron tick enqueue | Leader cron → `RunQueue.Enqueue` (scheduler pod) |

## 1. Pod topology (homogeneous or split)

**Intent:** The library does not require a single deployment shape. It supports both “every pod does everything” and deliberate split roles.

| Mode | Typical use |
|------|-------------|
| **Full pod** | Register jobs, run worker pool, run in-pod leader (Redis → local queue). |
| **Scheduler pod** | Register jobs only (or register + optional light workers if you enable them). |
| **Worker pod** | Worker pool + in-pod leader; skip job registration (or no-op register). |

**Rules:**

- No special pod type is required at the framework level—only **configuration** (which steps you start).
- **Job registration is idempotent** keyed by stable job id (and optionally schedule/version). Any number of pods may call register on startup; duplicates converge to one definition in Redis.
- Registration must not delete or overwrite a newer definition from another pod without an explicit version/compare rule (document your idempotency key semantics in the register API).

**Non-goals:** The library does not assign “scheduler” vs “worker” labels in Redis; the operator chooses which processes call `register`, `startWorkers`, and `startLeader`.

Use `StartWith(ctx, SchedulerPodMode())`, `WorkerPodMode()`, or `FullStartMode()`.

---

## 2. Startup (register and worker pool)

**Intent:** Ordering is **allowed and documented**, not **enforced**. Users of the library control startup per pod role.

### Recommended full pod

```text
1. registerJobs(ctx)     // idempotent; retry/backoff on Redis errors
2. startWorkerPool(ctx)  // local queue + workers exist before leader enqueues
3. startLeader(ctx)      // Redis claim → local queue
```

### Split pods

| Pod kind | Start |
|----------|--------|
| Scheduler | `registerJobs` (required for that deployment); worker pool / leader optional. |
| Worker | `startWorkerPool` then `startLeader`; omit `registerJobs` or use a no-op. |

**Library behavior:**

- Expose independent lifecycle hooks; do not assume all three run in one process.
- **Multi-register:** Safe when every registering pod uses the same idempotent register API (see §1).
- If a worker pod starts before any scheduler has registered jobs, workers simply idle until job metadata exists—no crash requirement.

**Not guaranteed:** Global “register completed everywhere before any worker runs” ordering across the cluster (operator responsibility for split deployments).

---

## 3. In-pod leader and local queue

**Intent:** Work moves **Redis → local queue → worker goroutines** inside each worker-capable pod.

- Exactly one **claimer loop per pod** (the “worker leader”) should perform Redis claims and enqueue locally, unless you explicitly document a different concurrency model.
- Workers consume only from the local queue (or equivalent in-process buffer), not directly from Redis.
- Claims must use **atomic Redis semantics** (single script or command sequence with clear ownership/lease).
- If a pod dies after claim but before successful processing, **lease / visibility timeout** must eventually return work to the claimable set in Redis (standard at-least-once execution).

**Split topology:** Scheduler-only pods do not run a leader or worker pool unless configured to.

---

## 4. Backpressure and failure domains (local queue vs Redis)

**Intent:** Local queue fullness must not silently lose tasks. Redis capacity exhaustion is an **accepted** loss domain.

### Recommended pattern (implement this)

Use **reserve-then-claim** to avoid TOCTOU between “queue has room” and “claim succeeded”:

```text
1. Try acquire local slot (semaphore / bounded channel reservation).
2. If no slot: do not claim; backoff (leader idle or short sleep).
3. If slot acquired: atomic claim in Redis.
4. If claim fails: release slot; retry/backoff.
5. If claim succeeds: enqueue locally; on enqueue failure, NACK/requeue in Redis and release slot.
6. Worker completes job → ack/release lease in Redis (per your execution model).
```

### Local queue full (all worker pods)

| Situation | Required behavior |
|-----------|-------------------|
| This pod’s queue full | Do not claim (preferred) or claim then **requeue** to Redis; never keep a task only in memory without Redis backing. |
| All pods’ queues full | Tasks **remain in Redis** (backlog grows); leaders backoff. No task loss due to local saturation alone. |

### Redis RAM / memory full (explicit exception)

When Redis rejects writes or evicts data because **Redis itself is out of memory**:

- The library **may lose jobs** (register, enqueue, requeue, or ack paths may fail irrecoverably).
- Required observability: `Metrics.IncRedisOOM()` and error logs on OOM paths.

**Summary:**

| Failure | Task loss |
|---------|-----------|
| Local queue full (any/all pods) | **Not allowed** (backoff + requeue / no-claim). |
| Pod crash after claim | **Not allowed** (lease → reclaim). |
| Redis OOM / RAM full | **Allowed** (best-effort; no durability guarantee). |

---

## Implementation checklist (gorediscron)

- [x] Idempotent `Register` with version-guarded Redis metadata.
- [x] `StartWith` / `StartWorkers` / cron leader enqueue (split roles).
- [x] `internal.LocalQueue` + claim loop reserve-then-claim.
- [x] Redis pending/processing + lease ZSET + `ReclaimExpired`.
- [x] Requeue on active-lock contention and claim-loop failures.
- [x] `Metrics` interface (claims, requeues, acks, OOM, queue depth).
