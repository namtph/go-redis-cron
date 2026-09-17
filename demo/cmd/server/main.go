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
		namespace  = flag.String("namespace", "demo", "Redis key namespace")
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

	sched, err := gorediscron.New(rdb, gorediscron.Config{
		Namespace:  *namespace,
		InstanceID: *instanceID,
		LeaseTTL:   10 * time.Second,
	})
	if err != nil {
		log.Fatal(err)
	}

	if err := sched.Register(gorediscron.CronJobScheduler{
		Name: "heartbeat",
		Cron: "*/10 * * * * *",
		Fn: func(ctx context.Context) error {
			log.Printf("[%s] heartbeat job", *instanceID)
			return nil
		},
	}); err != nil {
		log.Fatal(err)
	}

	if err := sched.StartWorkers(2); err != nil {
		log.Fatal(err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	if err := sched.Start(ctx); err != nil {
		log.Fatal(err)
	}
	defer func() {
		_ = sched.Stop(context.Background())
	}()

	mux := http.NewServeMux()
	mux.Handle("/", ui.Handler(sched, ui.Options{Prefix: *uiPrefix}))

	srv := &http.Server{Addr: *addr, Handler: mux}

	log.Printf("demo listening on %s instance=%s ui=%s", *addr, *instanceID, *uiPrefix)

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
