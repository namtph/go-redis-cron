# scheduler-pod

Split deployment: **register jobs** and run **cron leader election** only. Enqueued runs are stored in Redis for worker pods to claim.

Start Redis, then run this process before (or alongside) [worker-pod](../worker-pod/).

```bash
docker compose up -d
go run . -instance scheduler-1
```

Use the same `-namespace` as worker-pod (default `examples-split`). Dashboard: http://localhost:8080/cron/
