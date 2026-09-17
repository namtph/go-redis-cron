package gorediscron

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/namtph/go-redis-cron/gorediscron/internal"
	"github.com/redis/go-redis/v9"
)

func TestRegisterJobNoopSameDefinition(t *testing.T) {
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})

	r, err := New(rdb, Config{Namespace: "v2", InstanceID: "a", LeaseTTL: 2 * time.Second})
	if err != nil {
		t.Fatal(err)
	}

	job := CronJob{Name: "tick", TaskName: "work", Cron: "* * * * *"}
	if err := r.RegisterJob(job); err != nil {
		t.Fatal(err)
	}
	rec, ok := r.registry.get("tick")
	if !ok || rec.version != 1 {
		t.Fatalf("expected version 1, got %+v", rec)
	}

	if err := r.RegisterJob(job); err != nil {
		t.Fatal(err)
	}
	rec, ok = r.registry.get("tick")
	if !ok || rec.version != 1 {
		t.Fatalf("expected noop version 1, got %+v", rec)
	}
}

func TestRegisterJobBumpsVersionWhenCronChanges(t *testing.T) {
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})

	r, err := New(rdb, Config{Namespace: "v2", InstanceID: "a", LeaseTTL: 2 * time.Second})
	if err != nil {
		t.Fatal(err)
	}

	_ = r.RegisterTask("work", func(ctx context.Context, args ...any) error { return nil })

	if err := r.RegisterJob(CronJob{Name: "tick", TaskName: "work", Cron: "* * * * *"}); err != nil {
		t.Fatal(err)
	}
	if err := r.RegisterJob(CronJob{Name: "tick", TaskName: "work", Cron: "*/2 * * * *"}); err != nil {
		t.Fatal(err)
	}
	rec, ok := r.registry.get("tick")
	if !ok || rec.version != 2 || rec.cron != "*/2 * * * *" {
		t.Fatalf("unexpected record: %+v", rec)
	}
}

func TestRegisterJobSixFieldCron(t *testing.T) {
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})

	r, err := New(rdb, Config{Namespace: "v2", InstanceID: "a", LeaseTTL: 2 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	_ = r.RegisterTask("work", func(ctx context.Context, args ...any) error { return nil })

	if err := r.RegisterJob(CronJob{Name: "fast", TaskName: "work", Cron: "*/1 * * * * *"}); err != nil {
		t.Fatalf("six-field cron: %v", err)
	}
	rec, ok := r.registry.get("fast")
	if !ok || rec.entryID == 0 {
		t.Fatalf("expected cron entry, got %+v", rec)
	}
}

func TestUnknownTaskSkipsRun(t *testing.T) {
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})

	r, err := New(rdb, Config{Namespace: "v2", InstanceID: "a", LeaseTTL: 2 * time.Second})
	if err != nil {
		t.Fatal(err)
	}

	if err := r.RegisterJob(CronJob{Name: "tick", TaskName: "missing", Cron: "@every 1h"}); err != nil {
		t.Fatal(err)
	}
	rec, _ := r.registry.get("tick")
	ctx := context.Background()
	r.handleRun(ctx, internal.RunTask{
		Name:        "tick",
		TaskName:    "missing",
		Version:     rec.version,
		ScheduledAt: time.Now().UnixMilli(),
	})
	rec, _ = r.registry.get("tick")
	if rec.lastStatus != RunStatusSkipped {
		t.Fatalf("expected skipped, got %s", rec.lastStatus)
	}
}
