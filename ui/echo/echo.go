package uiecho

import (
	"github.com/labstack/echo/v4"
	"github.com/namtph/go-redis-cron/ui"
)

// Mount registers the job dashboard and API on an Echo instance.
func Mount(e *echo.Echo, src ui.JobSource, opts ui.Options) {
	h := ui.Handler(src, opts)
	prefix := ui.URLPrefix(opts)
	wrap := echo.WrapHandler(h)
	e.Any(prefix, wrap)
	e.Any(prefix+"/*", wrap)
}
