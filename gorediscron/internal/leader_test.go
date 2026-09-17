package internal

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

func TestLeaderSingleInstance(t *testing.T) {
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})

	l := NewLeader(rdb, "test", "pod-a", 2*time.Second)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		_ = l.Run(ctx)
	}()

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if l.IsLeader() {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatal("expected pod-a to become leader")
}

func TestLeaderOnlyOneWinner(t *testing.T) {
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})

	a := NewLeader(rdb, "test", "pod-a", 2*time.Second)
	b := NewLeader(rdb, "test", "pod-b", 2*time.Second)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() { _ = a.Run(ctx) }()
	go func() { _ = b.Run(ctx) }()

	time.Sleep(500 * time.Millisecond)

	leaders := 0
	if a.IsLeader() {
		leaders++
	}
	if b.IsLeader() {
		leaders++
	}
	if leaders != 1 {
		t.Fatalf("expected exactly one leader, got %d", leaders)
	}
}
