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

	rt, err := gorediscron.New(rdb, gorediscron.Config{
		Namespace:  *namespace,
		InstanceID: *instanceID,
		LeaseTTL:   10 * time.Second,
	})
	if err != nil {
		log.Fatal(err)
	}

	if err := rt.RegisterTask("split-heartbeat", func(ctx context.Context, args ...any) error {
		log.Printf("[%s] split-heartbeat executed on worker pod", *instanceID)
		return nil
	}); err != nil {
		log.Fatal(err)
	}
	if err := rt.RegisterJob(gorediscron.CronJob{
		Name:     "split-heartbeat",
		TaskName: "split-heartbeat",
		Cron:     "*/10 * * * * *",
	}); err != nil {
		log.Fatal(err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	pool := gorediscron.WorkerPoolConfig{NumberOfWorkerInstances: *workers}
	if err := rt.StartWith(ctx, gorediscron.WorkerPodMode(pool)); err != nil {
		log.Fatal(err)
	}
	defer func() {
		_ = rt.Stop(context.Background())
	}()

	log.Printf("worker-pod running instance=%s workers=%d (waiting for claims)", *instanceID, *workers)
	<-ctx.Done()
}
