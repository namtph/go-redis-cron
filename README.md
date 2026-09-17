# go-redis-cron

A small Go library for running cron schedules across multiple pods with Redis-backed leader election, plus an optional embedded job dashboard.

## Repository layout

| Area | Path | Description |
|------|------|-------------|
| **Scheduler** | [`gorediscron/`](gorediscron/) | Core library: Redis leader election + cron execution |
| **Job UI** | [`ui/`](ui/) | Embedded static dashboard and JSON API; mounts for Gin, Echo, and mux |
| **Demo** | [`demo/`](demo/) | Local Redis + sample HTTP server to test multi-instance behavior |

## Problem

When you run the same service in multiple replicas, a plain in-process cron scheduler fires on every pod. That duplicates work, wastes resources, and can corrupt shared state.

`go-redis-cron` coordinates replicas through Redis so exactly one pod runs scheduled jobs at a time, with automatic failover when the leader stops renewing its lease.

## Features

- Cron schedules via [robfig/cron](https://github.com/robfig/cron) (5- or 6-field expressions)
- Redis leader election with lease renewal (`SET NX` + TTL heartbeat)
- Safe to call `Start()` on every pod; non-leaders skip job execution
- Built-in static job viewer (`GET /cron/` by default)
- Mount helpers: `ui/gin`, `ui/echo`, `ui/mux`

## Requirements

- Go 1.22+
- Redis 6+ (standalone, Sentinel, or Cluster with hash-tagged keys)

## Installation

```bash
go get github.com/namtph/go-redis-cron/gorediscron
```

## Scheduler quick start

```go
package main

import (
	"context"
	"log"
	"time"

	"github.com/namtph/go-redis-cron/gorediscron"
	"github.com/redis/go-redis/v9"
)

func main() {
	rdb := redis.NewClient(&redis.Options{Addr: "localhost:6379"})

	sched, err := gorediscron.New(rdb, gorediscron.Config{
		Namespace:  "billing",
		InstanceID: "billing-pod-abc123",
		LeaseTTL:   10 * time.Second,
	})
	if err != nil {
		log.Fatal(err)
	}

	err = sched.AddFunc("hourly-report", "Hourly report", "0 * * * *", func(ctx context.Context) error {
		log.Println("hourly job ran once cluster-wide")
		return nil
	})
	if err != nil {
		log.Fatal(err)
	}

	if err := sched.Start(context.Background()); err != nil {
		log.Fatal(err)
	}
	defer sched.Stop(context.Background())
}
```

## Job UI

The `ui` package embeds a small static site and `GET …/api/jobs` JSON. Pass any `JobSource` (the scheduler implements `Jobs()`).

**stdlib / custom router** — use `ui.Handler`:

```go
http.Handle("/cron/", ui.Handler(sched, ui.Options{Prefix: "/cron"}))
```

**Gin**

```go
import uigin "github.com/namtph/go-redis-cron/ui/gin"

uigin.Mount(r, sched, ui.Options{Prefix: "/cron"})
```

**Echo** — `github.com/namtph/go-redis-cron/ui/echo`  
**mux** — `github.com/namtph/go-redis-cron/ui/mux`

See [`demo/cmd/server`](demo/cmd/server/main.go) for a working example with `-framework gin|echo|mux`.

## Demo

```bash
make redis
go run ./demo/cmd/server -instance demo-1 -addr :8080
```

Open http://localhost:8080/cron/ — details in [`demo/README.md`](demo/README.md).

## How it works

1. Each pod tries to acquire a namespaced leader key in Redis.
2. The leader renews the lease on an interval shorter than `LeaseTTL`.
3. Only the leader runs cron callbacks; followers mark runs as skipped.
4. On demotion or shutdown, the pod stops holding the lease so another pod can take over.

## Development

```bash
make test
make vet
```

## License

MIT
