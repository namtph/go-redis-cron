package gorediscron

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

func TestWorkerPoolBeforeRegister(t *testing.T) {
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	s, err := New(rdb, Config{Namespace: "t", InstanceID: "a", LeaseTTL: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := s.StartWorkerPool(ctx); err != nil {
		t.Fatal(err)
	}
	fn := func(ctx context.Context) error { return nil }
	if err := s.Register(CronJobScheduler{Name: "job", Cron: "* * * * *", Fn: fn}); err != nil {
		t.Fatal(err)
	}
	if err := s.Register(CronJobScheduler{Name: "job", Cron: "*/2 * * * *", Fn: fn}); err != nil {
		t.Fatal(err)
	}
}

func TestRegisterWithoutStart(t *testing.T) {
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	s, err := New(rdb, Config{Namespace: "t", InstanceID: "a", LeaseTTL: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	fn := func(ctx context.Context) error { return nil }
	if err := s.Register(CronJobScheduler{Name: "only", Cron: "@every 1h", Fn: fn}); err != nil {
		t.Fatal(err)
	}
}

func TestStartCronWithEmptyRegistry(t *testing.T) {
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	s, err := New(rdb, Config{Namespace: "t", InstanceID: "a", LeaseTTL: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := s.StartCron(ctx); err != nil {
		t.Fatal(err)
	}
}
