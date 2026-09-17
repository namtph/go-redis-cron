package internal

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

func TestRunClaimDedup(t *testing.T) {
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	q := NewRunQueue(rdb, "ns", nil)

	ctx := context.Background()
	at := time.Now().UnixMilli()

	ok1, err := q.TryClaimRun(ctx, "ns", "pod-a", "job", at)
	if err != nil || !ok1 {
		t.Fatalf("first claim: ok=%v err=%v", ok1, err)
	}
	ok2, err := q.TryClaimRun(ctx, "ns", "pod-b", "job", at)
	if err != nil || ok2 {
		t.Fatalf("second claim should fail: ok=%v err=%v", ok2, err)
	}
}
