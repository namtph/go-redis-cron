# AGENTS.md — go-redis-cron

## Project purpose

Go monorepo: **`gorediscron`** (distributed cron + Redis run queue + in-pod workers), **`ui`** (embedded dashboard), **`demo`** (Compose Redis + sample server).

**Non-goals:** generic workflow engine, delayed jobs outside cron, storing job funcs in Redis (Fn remains in-process on each executing pod).

## Architecture (META-aligned)

```
Cron leader pod(s)                Worker-capable pod(s)
┌──────────────────┐             ┌─────────────────────────────┐
│ Leader election  │             │ Claim loop (1 per pod)      │
│ Cron → LPUSH     │──Redis──────│ reserve → claim → local Q   │
│ Register → Redis │   pending   │ Workers → Fn                │
└──────────────────┘             └─────────────────────────────┘
```

See [docs/META.md](docs/META.md) for topology, startup order, backpressure, and Redis OOM policy.

## Public API (scheduler)

```go
func New(rdb redis.UniversalClient, cfg Config) (*Scheduler, error)
func (s *Scheduler) Register(def CronJobScheduler) error
func (s *Scheduler) StartWorkers(n int) error
func (s *Scheduler) Start(ctx context.Context) error
func (s *Scheduler) StartWith(ctx context.Context, mode StartMode) error
func (s *Scheduler) Stop(ctx context.Context) error
func (s *Scheduler) Jobs() []Job
```

## Coding conventions

- Go 1.22+, `context.Context` on lifecycle and job callbacks
- No inline imports; exhaustive switches on enums
- miniredis for Redis tests
- Run `go test ./...` and `go vet ./...` before commit

## Agent workflow

1. Keep scheduler / ui / demo diffs scoped.
2. Update README when public API changes.
3. Align behavior changes with docs/META.md.
