# AGENTS.md — go-redis-cron

## Project purpose

Go monorepo:

1. **`gorediscron`** — distributed cron scheduler (Redis leader election, run queue, in-pod workers).
2. **`ui`** — embedded static job dashboard + JSON API (`http.Handler` only).
3. **`examples/`** — standalone runnable programs (each with `main`, `go.mod`, and `docker-compose.yml`).

**Non-goals:** general-purpose task queue, delayed jobs, workflow engine as a separate product.

## Repository layout

```
gorediscron/          # import: github.com/namtph/go-redis-cron/gorediscron
ui/                   # import: github.com/namtph/go-redis-cron/ui
examples/
  full-pod/           # register + workers + UI on one process
  scheduler-pod/      # cron leader + job registration only
  worker-pod/         # worker pool + claim loop only
docs/META.md          # runtime contract
```

Each example uses `replace github.com/namtph/go-redis-cron => ../..` in its `go.mod`.

## Dependencies

| Module | Use |
|--------|-----|
| `github.com/redis/go-redis/v9` | Redis client |
| `github.com/robfig/cron/v3` | Cron parsing |
| `github.com/alicebob/miniredis/v2` | Tests |

No Gin/Echo in the library; examples use `net/http` only.

## Coding conventions

- Go 1.22+, `context.Context` on lifecycle and job callbacks
- No inline imports; exhaustive switches on enums/unions
- Table-driven tests; `go test ./...` and `go vet ./...` from repo root

## Agent workflow

1. **Default branch:** At the start of every task, run `git checkout dev` and `git pull origin dev`, unless the user or task gives different branch instructions.
2. **No direct pushes to `dev`:** Create a branch `cursor/<short-description>-eb22`, commit there, push the branch, and open a **pull request into `dev`** for the user to merge. Do not `git push origin dev` unless the user explicitly asks.
3. Read existing code before adding types or dependencies.
4. Prefer focused diffs; keep scheduler / ui / examples scoped.
5. Run `go test ./...` and `go vet ./...` before finishing.
6. Update `README.md` and `examples/README.md` when public API or example flags change.
7. Do not commit secrets, `.env` files, or local Redis dumps.

## Public API (v2 — see docs/META.md)

`Runtime` / `Scheduler` (alias). Decoupled **tasks**, **jobs**, **worker pool**; shared Redis prefix.

```go
func New(rdb redis.UniversalClient, cfg Config) (*Runtime, error)

func (r *Runtime) RegisterTask(name string, fn TaskFunc) error
func (r *Runtime) RegisterJob(job CronJob) error

func (r *Runtime) JoinLeader(ctx context.Context) error
func (r *Runtime) StartWorkerPool(ctx context.Context, cfg WorkerPoolConfig) error
func (r *Runtime) Stop(ctx context.Context) error
```

Job definition id: `CronJobID(taskName, cron)` — same id → noop; different id for same `job.Name` → overwrite + version bump.

See [docs/META.md](docs/META.md).

## Leader election (remember)

`JoinLeader`: one goroutine per pod. **Winner stays leader** until `Stop`, crash, or restart stops lease renewal—then another pod wins on the next round. Do not demote on transient Redis errors; renew until the key is lost or released.
