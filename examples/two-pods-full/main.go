package main

import (
	"context"
	"fmt"
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

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func main() {
	redisAddr := envOr("REDIS_ADDR", "localhost:6379")
	namespace := envOr("NAMESPACE", "two-pods-demo")
	instanceID := envOr("INSTANCE_ID", "")
	httpAddr := envOr("HTTP_ADDR", ":8080")
	uiPrefix := envOr("UI_PREFIX", "/cron")

	if instanceID == "" {
		host, err := os.Hostname()
		if err != nil {
			log.Fatal(err)
		}
		instanceID = host
	}

	rdb := redis.NewClient(&redis.Options{Addr: redisAddr})
	if err := rdb.Ping(context.Background()).Err(); err != nil {
		log.Fatalf("redis ping: %v", err)
	}

	rt, err := gorediscron.New(rdb, gorediscron.Config{
		Namespace:  namespace,
		InstanceID: instanceID,
		LeaseTTL:   10 * time.Second,
	})
	if err != nil {
		log.Fatal(err)
	}

	if err := rt.RegisterTask("alpha", func(ctx context.Context, args ...any) error {
		log.Printf("[%s] TASK alpha: hello from alpha handler %v", instanceID, args)
		return nil
	}); err != nil {
		log.Fatal(err)
	}
	if err := rt.RegisterTask("beta", func(ctx context.Context, args ...any) error {
		log.Printf("[%s] TASK beta: hello from beta handler %v", instanceID, args)
		return nil
	}); err != nil {
		log.Fatal(err)
	}

	// Four jobs, all fire every second (6-field cron) for easy log watching.
	jobs := []gorediscron.CronJob{
		{Name: "alpha-fast-1", TaskName: "alpha", Cron: "*/1 * * * * *", Args: []any{"job=alpha-fast-1"}},
		{Name: "alpha-fast-2", TaskName: "alpha", Cron: "*/1 * * * * *", Args: []any{"job=alpha-fast-2"}},
		{Name: "beta-fast-1", TaskName: "beta", Cron: "*/1 * * * * *", Args: []any{"job=beta-fast-1"}},
		{Name: "beta-fast-2", TaskName: "beta", Cron: "*/1 * * * * *", Args: []any{"job=beta-fast-2"}},
	}
	for _, job := range jobs {
		if err := rt.RegisterJob(job); err != nil {
			log.Fatal(err)
		}
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
	mux.Handle("/", ui.Handler(rt, ui.Options{Prefix: uiPrefix}))
	srv := &http.Server{Addr: httpAddr, Handler: mux}

	log.Printf("two-pods-full instance=%s redis=%s http=%s ui=%s jobs=%d tasks=2",
		instanceID, redisAddr, httpAddr, uiPrefix, len(jobs))
	fmt.Println("leader + workers running; watch logs for TASK alpha/beta lines every second")

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
