package main

import (
	"context"
	"flag"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/gorilla/mux"
	"github.com/labstack/echo/v4"
	"github.com/namtph/go-redis-cron/gorediscron"
	"github.com/namtph/go-redis-cron/ui"
	uiecho "github.com/namtph/go-redis-cron/ui/echo"
	uigin "github.com/namtph/go-redis-cron/ui/gin"
	uimux "github.com/namtph/go-redis-cron/ui/mux"
	"github.com/redis/go-redis/v9"
)

func main() {
	var (
		redisAddr  = flag.String("redis", "localhost:6379", "Redis address")
		namespace  = flag.String("namespace", "demo", "Redis key namespace")
		instanceID = flag.String("instance", "", "Unique instance id (default hostname)")
		addr       = flag.String("addr", ":8080", "HTTP listen address")
		framework  = flag.String("framework", "gin", "HTTP router: gin, echo, or mux")
		uiPrefix   = flag.String("ui-prefix", "/cron", "Dashboard URL prefix")
	)
	flag.Parse()

	if *instanceID == "" {
		host, err := os.Hostname()
		if err != nil {
			log.Fatal(err)
		}
		*instanceID = host
	}

	rdb := redis.NewClient(&redis.Options{Addr: *redisAddr})
	if err := rdb.Ping(context.Background()).Err(); err != nil {
		log.Fatalf("redis ping: %v", err)
	}

	sched, err := gorediscron.New(rdb, gorediscron.Config{
		Namespace:  *namespace,
		InstanceID: *instanceID,
		LeaseTTL:   10 * time.Second,
	})
	if err != nil {
		log.Fatal(err)
	}

	if err := sched.AddFunc("heartbeat", "Heartbeat", "*/10 * * * * *", func(ctx context.Context) error { // every 10s (6-field cron)
		log.Printf("[%s] heartbeat job", *instanceID)
		return nil
	}); err != nil {
		log.Fatal(err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	if err := sched.Start(ctx); err != nil {
		log.Fatal(err)
	}
	defer func() {
		_ = sched.Stop(context.Background())
	}()

	uiOpts := ui.Options{Prefix: *uiPrefix}
	srv := newServer(*framework, sched, uiOpts)

	log.Printf("demo listening on %s (%s) instance=%s ui=%s", *addr, *framework, *instanceID, *uiPrefix)

	go func() {
		if err := srv.ListenAndServe(*addr); err != nil && err != http.ErrServerClosed {
			log.Fatal(err)
		}
	}()

	<-ctx.Done()
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = srv.Shutdown(shutdownCtx)
}

type httpServer interface {
	ListenAndServe(addr string) error
	Shutdown(ctx context.Context) error
}

func newServer(framework string, sched *gorediscron.Scheduler, uiOpts ui.Options) httpServer {
	switch framework {
	case "gin":
		gin.SetMode(gin.ReleaseMode)
		r := gin.New()
		r.Use(gin.Recovery())
		uigin.Mount(r, sched, uiOpts)
		return &ginServer{engine: r}
	case "echo":
		e := echo.New()
		e.HideBanner = true
		uiecho.Mount(e, sched, uiOpts)
		return &echoServer{echo: e}
	case "mux":
		r := mux.NewRouter()
		uimux.Mount(r, sched, uiOpts)
		return &stdlibServer{handler: r}
	default:
		log.Fatalf("unknown framework %q (use gin, echo, or mux)", framework)
		return nil
	}
}

type ginServer struct {
	engine *gin.Engine
	server *http.Server
}

func (s *ginServer) ListenAndServe(addr string) error {
	s.server = &http.Server{Addr: addr, Handler: s.engine}
	return s.server.ListenAndServe()
}

func (s *ginServer) Shutdown(ctx context.Context) error {
	if s.server == nil {
		return nil
	}
	return s.server.Shutdown(ctx)
}

type echoServer struct {
	echo *echo.Echo
}

func (s *echoServer) ListenAndServe(addr string) error {
	return s.echo.Start(addr)
}

func (s *echoServer) Shutdown(ctx context.Context) error {
	return s.echo.Shutdown(ctx)
}

type stdlibServer struct {
	handler http.Handler
	server  *http.Server
}

func (s *stdlibServer) ListenAndServe(addr string) error {
	s.server = &http.Server{Addr: addr, Handler: s.handler}
	return s.server.ListenAndServe()
}

func (s *stdlibServer) Shutdown(ctx context.Context) error {
	if s.server == nil {
		return nil
	}
	return s.server.Shutdown(ctx)
}
