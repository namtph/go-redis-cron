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
		namespace  = flag.String("namespace", "full-pod", "Redis key namespace")
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

	if err := rt.RegisterTask("heartbeat", func(ctx context.Context, args ...any) error {
		log.Printf("[%s] heartbeat job", *instanceID)
		return nil
	}); err != nil {
		log.Fatal(err)
	}
	if err := rt.RegisterJob(gorediscron.CronJob{
		Name:     "heartbeat",
		TaskName: "heartbeat",
		Cron:     "*/10 * * * * *",
	}); err != nil {
		log.Fatal(err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	pool := gorediscron.WorkerPoolConfig{NumberOfWorkerInstances: 2}
	if err := rt.StartWith(ctx, gorediscron.FullStartMode(pool)); err != nil {
		log.Fatal(err)
	}
	defer func() {
		_ = rt.Stop(context.Background())
	}()

	mux := http.NewServeMux()
	mux.Handle("/", ui.Handler(rt, ui.Options{Prefix: *uiPrefix}))

	srv := &http.Server{Addr: *addr, Handler: mux}

	log.Printf("full-pod listening on %s instance=%s ui=%s", *addr, *instanceID, *uiPrefix)

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
