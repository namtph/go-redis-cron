package internal

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

// Leader coordinates Redis-based leader election for one scheduler namespace.
type Leader struct {
	rdb        redis.UniversalClient
	key        string
	instanceID string
	leaseTTL   time.Duration
	renewEvery time.Duration

	mu      sync.RWMutex
	leader  bool
	stopCh     chan struct{}
	doneCh     chan struct{}
	started    bool
	stopOnce   sync.Once
	onPromoted func()
	onDemoted  func()
}

// NewLeader creates a leader elector. Call Run to participate in election.
func NewLeader(rdb redis.UniversalClient, namespace, instanceID string, leaseTTL time.Duration) *Leader {
	renew := leaseTTL / 3
	if renew < time.Second {
		renew = time.Second
	}
	return &Leader{
		rdb:        rdb,
		key:        LeaderKey(namespace),
		instanceID: instanceID,
		leaseTTL:   leaseTTL,
		renewEvery: renew,
		stopCh:     make(chan struct{}),
		doneCh:     make(chan struct{}),
	}
}

// SetCallbacks runs onPromoted when this instance wins the lease and onDemoted when it loses it.
// Set before Run; callbacks must be non-blocking.
func (l *Leader) SetCallbacks(onPromoted, onDemoted func()) {
	l.mu.Lock()
	l.onPromoted = onPromoted
	l.onDemoted = onDemoted
	l.mu.Unlock()
}

// IsLeader reports whether this instance currently holds the lease.
func (l *Leader) IsLeader() bool {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.leader
}

// Run acquires and renews the lease until ctx is canceled or Stop is called.
func (l *Leader) Run(ctx context.Context) error {
	l.mu.Lock()
	if l.started {
		l.mu.Unlock()
		return errors.New("leader: already running")
	}
	l.started = true
	l.mu.Unlock()

	defer close(l.doneCh)

	ticker := time.NewTicker(l.renewEvery)
	defer ticker.Stop()

	for {
		if err := l.tick(ctx); err != nil {
			return err
		}

		select {
		case <-ctx.Done():
			_ = l.release(context.Background())
			return ctx.Err()
		case <-l.stopCh:
			_ = l.release(context.Background())
			return nil
		case <-ticker.C:
		}
	}
}

// Stop ends the election loop and releases the lease when held.
func (l *Leader) Stop() {
	l.stopOnce.Do(func() {
		close(l.stopCh)
	})
	l.mu.Lock()
	started := l.started
	l.mu.Unlock()
	if started {
		<-l.doneCh
	}
}

func (l *Leader) tick(ctx context.Context) error {
	if l.IsLeader() {
		ok, err := l.renew(ctx)
		if err != nil {
			return err
		}
		if !ok {
			l.setLeader(false)
		}
		return nil
	}

	ok, err := l.acquire(ctx)
	if err != nil {
		return err
	}
	l.setLeader(ok)
	return nil
}

func (l *Leader) acquire(ctx context.Context) (bool, error) {
	ok, err := l.rdb.SetNX(ctx, l.key, l.instanceID, l.leaseTTL).Result()
	if err != nil {
		return false, fmt.Errorf("leader acquire: %w", err)
	}
	return ok, nil
}

var renewScript = redis.NewScript(`
if redis.call("GET", KEYS[1]) == ARGV[1] then
  return redis.call("PEXPIRE", KEYS[1], ARGV[2])
end
return 0
`)

func (l *Leader) renew(ctx context.Context) (bool, error) {
	ms := l.leaseTTL.Milliseconds()
	n, err := renewScript.Run(ctx, l.rdb, []string{l.key}, l.instanceID, ms).Int64()
	if err != nil {
		return false, fmt.Errorf("leader renew: %w", err)
	}
	return n == 1, nil
}

var releaseScript = redis.NewScript(`
if redis.call("GET", KEYS[1]) == ARGV[1] then
  return redis.call("DEL", KEYS[1])
end
return 0
`)

func (l *Leader) release(ctx context.Context) error {
	_, err := releaseScript.Run(ctx, l.rdb, []string{l.key}, l.instanceID).Result()
	if err != nil {
		return fmt.Errorf("leader release: %w", err)
	}
	l.setLeader(false)
	return nil
}

func (l *Leader) setLeader(v bool) {
	l.mu.Lock()
	was := l.leader
	l.leader = v
	promoted := l.onPromoted
	demoted := l.onDemoted
	l.mu.Unlock()

	if !was && v && promoted != nil {
		promoted()
	}
	if was && !v && demoted != nil {
		demoted()
	}
}
