# AGENTS.md — go-redis-cron

## Project purpose

Go monorepo with three deliverables:

1. **`gorediscron`** — distributed cron scheduler (Redis leader election + in-process cron).
2. **`ui`** — embedded static job dashboard + JSON API; optional mounts for Gin, Echo, and gorilla/mux.
3. **`demo`** — runnable server and Docker Compose Redis for manual and integration testing.

**Non-goals:** general-purpose task queue, delayed jobs, workflow engine, separate worker processes.

## Repository layout

```
gorediscron/          # import: github.com/namtph/go-redis-cron/gorediscron
  scheduler.go        # public Scheduler API
  registry.go         # in-memory job state for UI
  internal/           # leader election, Redis keys
ui/
  handler.go          # embed static + stdlib mux
  api.go              # GET /api/jobs
  static/             # index.html, app.js, app.css
  gin/ echo/ mux/     # framework mount helpers (optional deps)
demo/
  docker-compose.yml  # Redis 7
  cmd/server/         # flags: -framework, -instance, -redis, -ui-prefix
```

## Architecture

```
┌─────────────┐     ┌─────────────┐
│  Pod A      │     │  Pod B      │
│ Scheduler   │     │ Scheduler   │
│ + ui mount  │     │ + ui mount  │
└──────┬──────┘     └──────┬──────┘
       └──────────┬────────┘
                  ▼
           ┌─────────────┐
           │    Redis    │
           │ leader lock │
           └─────────────┘
```

Every pod may expose the UI (read-only job snapshot). Only the leader executes `JobFunc` callbacks.

### Leader election

- Acquire: `SET {ns}:leader instanceID NX PX ttl`
- Renew / release via Lua comparing `instanceID`
- Renewal interval ≈ `LeaseTTL / 3`
- Cluster: hash tag `{namespace}` on all keys touched in one script

### UI contract

- `ui.JobSource` → `Jobs() []gorediscron.Job`
- `*gorediscron.Scheduler` implements `JobSource`
- Default prefix `/cron` (static + `api/jobs` relative to prefix)

## Dependencies

| Module | Use |
|--------|-----|
| `github.com/redis/go-redis/v9` | Redis client |
| `github.com/robfig/cron/v3` | Cron parsing |
| `github.com/alicebob/miniredis/v2` | Tests |
| gin / echo / mux | Demo + `ui/*` mounts only |

## Coding conventions

- Go 1.22+, `context.Context` on `Start`, `Stop`, and job callbacks
- `JobFunc func(ctx context.Context) error`
- Register jobs with stable `id`, display `name`, and cron `spec`
- Table-driven tests; miniredis for leader tests
- No inline imports; exhaustive switches on enums/unions
- Keep framework adapters thin — delegate to `ui.Handler`

## Testing checklist

- [x] Leader: single winner among two instances (miniredis)
- [x] UI: `/api/jobs` returns registered jobs
- [ ] Scheduler: job runs only when leader
- [ ] Scheduler: failover promotes standby
- [ ] Demo: two instances, one leader in UI

## Agent workflow

1. Identify which area changed (scheduler / ui / demo) and keep diffs scoped.
2. Run `go test ./...` and `go vet ./...` from repo root.
3. Update `README.md` when public API or demo flags change.
4. Do not commit secrets or local Redis dumps.

## Public API (scheduler)

```go
func New(rdb redis.UniversalClient, cfg Config) (*Scheduler, error)
func (s *Scheduler) AddFunc(id, name, spec string, fn JobFunc) error
func (s *Scheduler) Jobs() []Job
func (s *Scheduler) Start(ctx context.Context) error
func (s *Scheduler) Stop(ctx context.Context) error
```
