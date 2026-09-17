package main

import (
	"context"
	"flag"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/namtph/go-redis-cron/gorediscron"
	"github.com/namtph/go-redis-cron/ui"
	"github.com/redis/go-redis/v9"
)

func main() {
	var (
		redisAddr  = flag.String("redis", "localhost:6379", "Redis address")
		namespace  = flag.String("namespace", "examples-split", "Redis key namespace (must match worker-pod)")
		instanceID = flag.String("instance", "", "Unique instance id (default hostname)")
		addr       = flag.String("addr", ":8080", "HTTP listen address")
		uiPrefix   = flag.String("ui-prefix", "/cron", "Dashboard URL prefix")
	)
	flag.Parse()

	if *instanceID == "" {
		host, err := os.Hostname()
		if err != nil {
			log.Fatal(err)
		}
		*instanceID = host
	}

	rdb := redis.NewClient(&redis.Options{Addr: *redisAddr})
	if err := rdb.Ping(context.Background()).Err(); err != nil {
		log.Fatalf("redis ping: %v", err)
	}

	rt, err := gorediscron.New(rdb, gorediscron.Config{
		Namespace:  *namespace,
		InstanceID: *instanceID,
		LeaseTTL:   10 * time.Second,
	})
	if err != nil {
		log.Fatal(err)
	}

	_ = rt.RegisterTask("split-heartbeat", func(ctx context.Context, args ...any) error { return nil })

	if err := rt.RegisterJob(gorediscron.CronJob{
		Name:     "split-heartbeat",
		TaskName: "split-heartbeat",
		Cron:     "*/10 * * * * *",
	}); err != nil {
		log.Fatal(err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	if err := rt.StartWith(ctx, gorediscron.SchedulerPodMode()); err != nil {
		log.Fatal(err)
	}
	defer func() {
		_ = rt.Stop(context.Background())
	}()

	mux := http.NewServeMux()
	mux.Handle("/", ui.Handler(rt, ui.Options{Prefix: *uiPrefix}))
	srv := &http.Server{Addr: *addr, Handler: mux}

	log.Printf("scheduler-pod listening on %s instance=%s ui=%s", *addr, *instanceID, *uiPrefix)

	go func() {
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatal(err)
		}
	}()

	<-ctx.Done()
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = srv.Shutdown(shutdownCtx)
}
