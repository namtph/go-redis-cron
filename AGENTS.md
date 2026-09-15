# AGENTS.md — go-redis-cron

## Project purpose

Small Go library: run cron schedules safely across multiple pods using Redis leader election. Every replica may start the scheduler; only the elected leader executes job callbacks.

**Non-goals:** distributed task queue, delayed jobs, workflow orchestration, HTTP workers. Keep the API minimal.

## Architecture

```
┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│   Pod A     │     │   Pod B     │     │   Pod C     │
│  Scheduler  │     │  Scheduler  │     │  Scheduler  │
│  + cron     │     │  + cron     │     │  + cron     │
└──────┬──────┘     └──────┬──────┘     └──────┬──────┘
       │                   │                   │
       └───────────────────┼───────────────────┘
                           ▼
                    ┌─────────────┐
                    │    Redis    │
                    │ leader lock │
                    │  (SET NX)   │
                    └─────────────┘
```

### Core components (target layout)

| Package / file | Responsibility |
|----------------|----------------|
| `scheduler.go` | Public `Scheduler`, `Config`, `New`, `AddFunc`, `Start`, `Stop` |
| `leader.go` | Acquire, renew, release lease; `OnPromoted` / `OnDemoted` hooks |
| `keys.go` | Namespaced Redis key helpers (`{namespace}:leader`, etc.) |
| `doc.go` | Package documentation for `go doc` |

### Leader election rules

- Acquire with `SET key instanceID NX PX <ttl>`.
- Renew only when value matches `InstanceID` (Lua or `GET` + conditional `PEXPIRE`).
- Renewal interval ≈ `LeaseTTL / 3`.
- On demotion: stop cron ticks immediately; do not run callbacks.
- On `Stop`: cancel context, release lease if leader, wait for in-flight job (with timeout).

### Redis Cluster

Keys touched in one atomic script must share a hash tag: `{namespace}:leader`, `{namespace}:fence`, etc.

## Dependencies

- `github.com/redis/go-redis/v9` — Redis client
- `github.com/robfig/cron/v3` — cron parsing and scheduling
- `github.com/alicebob/miniredis/v2` — unit/integration tests (test only)

Avoid heavy frameworks. Prefer stdlib + small, well-known libraries.

## Coding conventions

- Go 1.22+. Use `context.Context` on public APIs (`Start`, `Stop`, job callbacks).
- Job signature: `func(ctx context.Context) error`.
- Export only what users need; keep leader logic internal or in `internal/`.
- Table-driven tests; cover leader failover, duplicate `Start`, and graceful `Stop`.
- No inline imports. Exhaustive switches on enums/unions with `default: var _ T = x; panic("unhandled")` or equivalent.
- Errors: wrap with `%w`; return typed errors only when callers need to branch.

## Testing checklist

- [ ] Single instance acquires lease and runs job on schedule
- [ ] Second instance does not run jobs while first holds lease
- [ ] Lease expiry promotes standby within `LeaseTTL + renewal slack`
- [ ] `Stop` on leader allows follower to take over
- [ ] Duplicate `InstanceID` is rejected or documented as unsafe
- [ ] Namespace isolates keys between deployments

## Agent workflow

1. Read existing code before adding types or dependencies.
2. Prefer focused diffs; do not scaffold unrelated tooling unless asked.
3. Run `go test ./...` and `go vet ./...` before finishing.
4. Update README examples when the public API changes.
5. Do not commit secrets, `.env` files, or local Redis dumps.

## Public API sketch (stable target)

```go
type Config struct {
    Namespace  string
    InstanceID string
    LeaseTTL   time.Duration
    Logger     Logger // optional
}

func New(rdb redis.UniversalClient, cfg Config) (*Scheduler, error)
func (s *Scheduler) AddFunc(spec string, fn JobFunc) error
func (s *Scheduler) Start(ctx context.Context) error
func (s *Scheduler) Stop(ctx context.Context) error

type JobFunc func(ctx context.Context) error
```

Adjust names to match implementation, but keep this shape unless the user requests otherwise.
