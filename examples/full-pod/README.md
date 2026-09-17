# full-pod

Single process: job registration, worker pool, cron leader election, and embedded job UI.

## Run

```bash
docker compose up -d
go run . -instance pod-1 -addr :8080
```

Dashboard: http://localhost:8080/cron/

## Two replicas

```bash
go run . -instance pod-1 -addr :8080 &
go run . -instance pod-2 -addr :8081 &
```

Only one pod holds the cron leader lease; workers on each pod can execute claimed runs.
