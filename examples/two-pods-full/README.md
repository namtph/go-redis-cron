# two-pods-full

Full homogeneous pods: **Redis + two replicas**, each running leader election, workers, and the job UI.

- **2 tasks:** `alpha` and `beta` (print to stdout)
- **4 cron jobs:** all use `*/1 * * * * *` (every second) for easy log testing

| Job | Task |
|-----|------|
| `alpha-fast-1` | `alpha` |
| `alpha-fast-2` | `alpha` |
| `beta-fast-1` | `beta` |
| `beta-fast-2` | `beta` |

Only one pod is cron **leader** (enqueues ticks). Either pod may **execute** claimed runs.

## Docker Compose (Redis + 2 pods)

```bash
cd examples/two-pods-full
docker compose up --build
```

- Redis: `localhost:6379`
- Pod A UI: http://localhost:8080/cron/
- Pod B UI: http://localhost:8081/cron/

Follow logs:

```bash
docker compose logs -f pod-a pod-b
```

You should see `TASK alpha` / `TASK beta` lines roughly every second, with `instance_id` showing which pod ran the handler.

## Local (Redis in Compose, apps on host)

```bash
docker compose up -d redis
go run . # terminal 1
INSTANCE_ID=pod-b HTTP_ADDR=:8081 go run . # terminal 2
```
