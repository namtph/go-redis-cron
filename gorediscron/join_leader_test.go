package gorediscron

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

func TestJoinLeaderStartsCronWhenElected(t *testing.T) {
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})

	s, err := New(rdb, Config{Namespace: "join", InstanceID: "pod-a", LeaseTTL: 2 * time.Second})
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := s.JoinLeader(ctx); err != nil {
		t.Fatal(err)
	}

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if s.leader.IsLeader() && s.cronStarted.Load() {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatal("expected leader with cron started")
}
