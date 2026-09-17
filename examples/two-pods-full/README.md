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

## Docker Compose (Redis + 2 pods + LB)

Plain Compose does **not** load-balance two services on one port by itself. This stack uses **nginx** in front of `pod-a` and `pod-b` (same idea as Kubernetes Service or Swarm routing).

```bash
cd examples/two-pods-full
docker compose up --build
```

- Redis: `localhost:6379`
- Dashboard (LB): http://localhost:8080/cron/ — requests go to either pod

Refresh may hit a different backend; `leaderRun` in the JSON reflects **that** pod (only one is leader).

Follow task logs:

```bash
docker compose logs -f pod-a pod-b
```

## Local (Redis in Compose, apps on host)

```bash
docker compose up -d redis
go run . # terminal 1
INSTANCE_ID=pod-b HTTP_ADDR=:8081 go run . # terminal 2
```

Use two terminals or your own local proxy if you want one URL on the host.
