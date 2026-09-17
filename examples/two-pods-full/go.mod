module github.com/namtph/go-redis-cron/examples/two-pods-full

go 1.23.0

require (
	github.com/namtph/go-redis-cron v0.0.0
	github.com/redis/go-redis/v9 v9.14.0
)

require (
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/dgryski/go-rendezvous v0.0.0-20200823014737-9f7001d12a5f // indirect
	github.com/robfig/cron/v3 v3.0.1 // indirect
	golang.org/x/sync v0.8.0 // indirect
)

replace github.com/namtph/go-redis-cron => ../..
