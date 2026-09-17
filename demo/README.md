# Demo

Runnable environment to exercise the scheduler, leader election, and built-in job UI.

## Prerequisites

- Go 1.22+
- Docker (for Redis)

## Start Redis

```bash
docker compose up -d
```

## Run the demo server

```bash
# from repository root
go run ./demo/cmd/server \
  -redis localhost:6379 \
  -instance demo-1 \
  -addr :8080
```

Open http://localhost:8080/cron/

### Multiple replicas

Run two processes with different `-instance` values against the same Redis. Only one shows **leader** in the UI; cron callbacks run on the leader.

```bash
go run ./demo/cmd/server -instance demo-1 -addr :8080 &
go run ./demo/cmd/server -instance demo-2 -addr :8081 &
```

The demo uses `net/http` only; mount `ui.Handler` the same way in your own app or wrap it with your router’s `http.Handler` adapter.
