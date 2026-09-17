package gorediscron

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

func TestRegisterBumpsVersionOnOverwrite(t *testing.T) {
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})

	s, err := New(rdb, Config{
		Namespace:  "test",
		InstanceID: "a",
		LeaseTTL:   2 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}

	fn := func(ctx context.Context) error { return nil }
	if err := s.Register(CronJobScheduler{Name: "job", Cron: "* * * * *", Fn: fn}); err != nil {
		t.Fatal(err)
	}
	rec, ok := s.registry.get("job")
	if !ok || rec.version != 1 {
		t.Fatalf("expected version 1, got %v", rec)
	}

	if err := s.Register(CronJobScheduler{Name: "job", Cron: "*/2 * * * *", Fn: fn}); err != nil {
		t.Fatal(err)
	}
	rec, ok = s.registry.get("job")
	if !ok || rec.version != 2 || rec.cron != "*/2 * * * *" {
		t.Fatalf("unexpected record after overwrite: %+v", rec)
	}
}
