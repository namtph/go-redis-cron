# AGENTS.md — go-redis-cron

## Public API (scheduler)

```go
func New(rdb redis.UniversalClient, cfg Config) (*Scheduler, error)
func (s *Scheduler) Register(def CronJobScheduler) error
func (s *Scheduler) StartLeaderElection(ctx context.Context) error
func (s *Scheduler) StartCron(ctx context.Context) error
func (s *Scheduler) StartWorkerPool(ctx context.Context, cfg WorkerPoolConfig) error
func (s *Scheduler) Start(ctx context.Context) error
func (s *Scheduler) StartWith(ctx context.Context, mode StartMode) error
func (s *Scheduler) Stop(ctx context.Context) error
func (s *Scheduler) Jobs() []Job
```

`WorkerPoolConfig` holds `NumberOfWorkerInstances` (required ≥ 1 when starting the pool).

See [docs/META.md](docs/META.md).
