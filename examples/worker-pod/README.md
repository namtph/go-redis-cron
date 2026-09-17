# worker-pod

Split deployment: **claim loop + worker pool** only (no cron leader). Pair with [scheduler-pod](../scheduler-pod/) using the same Redis and `-namespace`.

```bash
docker compose up -d   # skip if Redis already running from scheduler-pod
go run . -instance worker-1
```

Register the same job names and `Fn` handlers as the scheduler process so claimed runs can execute.
