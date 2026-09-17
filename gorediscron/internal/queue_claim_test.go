package internal

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

func TestClaimPendingRequeue(t *testing.T) {
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	q := NewRunQueue(rdb, "ns", nil)
	ctx := context.Background()

	task := RunTask{Name: "job", Version: 1, ScheduledAt: time.Now().UnixMilli()}
	if err := q.Enqueue(ctx, task); err != nil {
		t.Fatal(err)
	}
	got, ok, err := q.ClaimPending(ctx, "pod-a")
	if err != nil || !ok {
		t.Fatalf("claim: ok=%v err=%v", ok, err)
	}
	if got.Name != "job" {
		t.Fatalf("got %+v", got)
	}
	if err := q.Requeue(ctx, got); err != nil {
		t.Fatal(err)
	}
	pending, err := rdb.LLen(ctx, RunQueueKey("ns")).Result()
	if err != nil || pending != 1 {
		t.Fatalf("pending=%d err=%v", pending, err)
	}
}
