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

Each call is independent. Typical pieces:

| Call | Purpose |
|------|---------|
| `Register` | Idempotent job + Redis metadata; anytime, repeat as needed |
| `SetWorkerCount(n)` | Configure worker goroutines (does not start them) |
| `StartLeaderElection(ctx)` | Redis leader loop |
| `StartCron(ctx)` | Cron engine (empty until you `Register`) |
| `StartWorkerPool(ctx)` | Claim loop + workers + reaper (default 1 worker if count unset) |
| `StartWith(ctx, mode)` | Optional convenience bundle |
| `Start(ctx)` | Leader election + cron only (not worker pool) |

```go
// Any order, e.g. workers first, register later:
_ = sched.StartWorkerPool(ctx)
_ = sched.Register(gorediscron.CronJobScheduler{...})
_ = sched.Register(gorediscron.CronJobScheduler{...}) // again later

// Or register only (persist metadata, no goroutines):
_ = sched.Register(gorediscron.CronJobScheduler{...})

// Full pod example:
_ = sched.SetWorkerCount(2)
_ = sched.StartWorkerPool(ctx)
_ = sched.StartLeaderElection(ctx)
_ = sched.StartCron(ctx)
_ = sched.Register(gorediscron.CronJobScheduler{Name: "hourly-report", Cron: "0 * * * *", Fn: fn})
```

## Split pods

```go
_ = sched.Register(...) // as needed
_ = sched.StartWith(ctx, gorediscron.SchedulerPodMode()) // election + cron, no workers

_ = sched.SetWorkerCount(4)
_ = sched.StartWith(ctx, gorediscron.WorkerPodMode()) // workers only
```

## How it works

1. Each pod may participate in leader election (scheduler pods).
2. The **cron leader** enqueues `RunTask` JSON into Redis pending.
3. On each worker-capable pod, one **claim loop** reserves a local slot, atomically claims from Redis, and enqueues locally.
4. Worker goroutines run `Fn` from the **local queue** only; skips requeue to Redis when appropriate (META §4).
5. Processing leases are reclaimed after TTL if a pod dies mid-claim.

## Development

```bash
make test
make vet
```

## License

MIT
