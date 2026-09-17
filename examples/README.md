# Examples

Each directory is a **complete runnable project**: `main` package, local `go.mod` (with a `replace` to the repo root), and `docker-compose.yml` for Redis.

| Example | What it demonstrates |
|---------|----------------------|
| [full-pod](full-pod/) | Register jobs, worker pool, leader election, cron, and embedded job UI in one process |
| [two-pods-full](two-pods-full/) | Docker Compose: Redis + **two** full pods, 2 tasks, 4 jobs every second |
| [scheduler-pod](scheduler-pod/) | Split deployment: register jobs + cron leader only (enqueue to Redis) |
| [worker-pod](worker-pod/) | Split deployment: claim loop + worker executors (no registration) |

## Quick start (full pod)

```bash
cd examples/full-pod
docker compose up -d
go run . -instance pod-1 -addr :8080
```

Open http://localhost:8080/cron/

## Split scheduler + worker

Use the same Redis and `-namespace` on both processes (default `examples-split`).

```bash
# Terminal 1 — Redis (either example folder works)
cd examples/scheduler-pod && docker compose up -d

# Terminal 2 — scheduler
cd examples/scheduler-pod
go run . -instance scheduler-1

# Terminal 3 — worker
cd examples/worker-pod
go run . -instance worker-1
```

Job logs appear on the **worker** process. The scheduler UI is at http://localhost:8080/cron/ when the scheduler example is running.

## Multiple replicas

Run two full-pod processes with different `-instance` and `-addr` values against the same Redis. Only one pod is cron leader; workers on every pod can execute claimed runs.
