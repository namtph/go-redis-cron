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
- Optional `Metrics` hook (claims, requeues, OOM, local queue depth)

## Scheduler quick start (full pod)

```go
sched, _ := gorediscron.New(rdb, gorediscron.Config{
	Namespace:  "billing",
	InstanceID: podName,
	LeaseTTL:   10 * time.Second,
})

_ = sched.Register(gorediscron.CronJobScheduler{
	Name: "hourly-report",
	Cron: "0 * * * *",
	Fn:   func(ctx context.Context) error { return nil },
})
_ = sched.StartWorkers(2)
_ = sched.Start(context.Background()) // FullStartMode: election + cron + workers
```

## Split pods

```go
// Scheduler pod: register + cron ticks only
_ = sched.StartWith(ctx, gorediscron.SchedulerPodMode())

// Worker pod: must Register locally for Fn; runs claim loop + workers
_ = sched.StartWorkers(4)
_ = sched.StartWith(ctx, gorediscron.WorkerPodMode())
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
