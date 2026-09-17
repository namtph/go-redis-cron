# AGENTS.md — go-redis-cron

## Project purpose

Go monorepo: **`gorediscron`** (distributed cron + Redis run queue + in-pod workers), **`ui`**, **`demo`**.

This is a **library**, not a framework: callers compose `Register`, `StartCron`, `StartWorkerPool`, etc. in any order.

## Public API (scheduler)

```go
func New(rdb redis.UniversalClient, cfg Config) (*Scheduler, error)
func (s *Scheduler) Register(def CronJobScheduler) error // anytime, repeatable
func (s *Scheduler) SetWorkerCount(n int) error
func (s *Scheduler) StartWorkers(n int) error             // alias for SetWorkerCount
func (s *Scheduler) StartLeaderElection(ctx context.Context) error
func (s *Scheduler) StartCron(ctx context.Context) error
func (s *Scheduler) StartWorkerPool(ctx context.Context) error
func (s *Scheduler) Start(ctx context.Context) error    // election + cron only
func (s *Scheduler) StartWith(ctx context.Context, mode StartMode) error
func (s *Scheduler) Stop(ctx context.Context) error
func (s *Scheduler) Jobs() []Job
```

See [docs/META.md](docs/META.md).

## Agent workflow

1. Keep diffs scoped (scheduler / ui / demo).
2. Run `go test ./...` and `go vet ./...`.
3. Do not add coupling between Register and Start* without explicit user request.
