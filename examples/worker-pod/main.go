package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/namtph/go-redis-cron/gorediscron"
	"github.com/redis/go-redis/v9"
)

func main() {
	var (
		redisAddr  = flag.String("redis", "localhost:6379", "Redis address")
		namespace  = flag.String("namespace", "examples-split", "Redis key namespace (must match scheduler-pod)")
		instanceID = flag.String("instance", "", "Unique instance id (default hostname)")
		workers    = flag.Int("workers", 2, "Worker goroutines in this pod")
	)
	flag.Parse()

	if *instanceID == "" {
		host, err := os.Hostname()
		if err != nil {
			log.Fatal(err)
		}
		*instanceID = host
	}
	if *workers < 1 {
		log.Fatal("-workers must be >= 1")
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

	// Workers need the same job name + Fn locally to execute claimed runs (Redis holds metadata only).
	if err := sched.Register(gorediscron.CronJobScheduler{
		Name: "split-heartbeat",
		Cron: "*/10 * * * * *",
		Fn: func(ctx context.Context) error {
			log.Printf("[%s] split-heartbeat executed on worker pod", *instanceID)
			return nil
		},
	}); err != nil {
		log.Fatal(err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	pool := gorediscron.WorkerPoolConfig{NumberOfWorkerInstances: *workers}
	if err := sched.StartWith(ctx, gorediscron.WorkerPodMode(pool)); err != nil {
		log.Fatal(err)
	}
	defer func() {
		_ = sched.Stop(context.Background())
	}()

	log.Printf("worker-pod running instance=%s workers=%d (waiting for claims)", *instanceID, *workers)
	<-ctx.Done()
}
