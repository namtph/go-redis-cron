# go-redis-cron

A small Go library for running cron schedules across multiple pods with Redis-backed leader election, a Redis run queue, in-pod backpressure, and an optional embedded job dashboard.

Design contract: [docs/META.md](docs/META.md).

## Repository layout

| Area | Path | Description |
|------|------|-------------|
| **Scheduler** | [`gorediscron/`](gorediscron/) | Core library: leader election, cron ticks, claim loop, workers |
| **Job UI** | [`ui/`](ui/) | Embedded static dashboard and JSON API (`net/http.Handler`) |
| **Demo** | [`demo/`](demo/) | Local Redis + sample HTTP server |

## Features

- Cron schedules via [robfig/cron](https://github.com/robfig/cron) (5- or 6-field expressions)
- Redis leader election; **only the leader enqueues** cron ticks to Redis
- **Worker pool on every pod**: in-pod claim loop (reserve → Redis claim → local queue) + executors
- Idempotent `Register` with version-guarded metadata in Redis
- Split deployments via `StartWith` (scheduler-only vs worker-only pods)
- **No enforced startup order** between `Register` and worker/cron/leader hooks
- Optional `Metrics` hook (claims, requeues, OOM, local queue depth)

## Scheduler API (library — no required order)

| Call | Purpose |
|------|---------|
| `Register` | Idempotent job + Redis metadata; anytime, repeat as needed |
| `StartLeaderElection(ctx)` | Redis leader loop |
| `StartCron(ctx)` | Cron engine (empty until you `Register`) |
| `StartWorkerPool(ctx, WorkerPoolConfig{NumberOfWorkerInstances: n})` | Claim loop + workers + reaper |
| `StartWith(ctx, mode)` | Optional convenience bundle |
| `Start(ctx)` | Leader election + cron only (not worker pool) |

```go
pool := gorediscron.WorkerPoolConfig{NumberOfWorkerInstances: 2}

_ = sched.StartWorkerPool(ctx, pool)
_ = sched.Register(gorediscron.CronJobScheduler{...})

_ = sched.StartLeaderElection(ctx)
_ = sched.StartCron(ctx)
```

## Split pods

```go
_ = sched.Register(...)
_ = sched.StartWith(ctx, gorediscron.SchedulerPodMode())

_ = sched.StartWith(ctx, gorediscron.WorkerPodMode(gorediscron.WorkerPoolConfig{NumberOfWorkerInstances: 4}))
```

## How it works

1. Each pod may participate in leader election (scheduler pods).
2. The **cron leader** enqueues `RunTask` JSON into Redis pending.
3. On each worker-capable pod, one **claim loop** reserves a local slot, atomically claims from Redis, and enqueues locally.
4. Worker goroutines run `Fn` from the **local queue** only.
5. Processing leases are reclaimed after TTL if a pod dies mid-claim.

## Development

```bash
make test
make vet
```

## License

MIT
