package uigin

import (
	"github.com/gin-gonic/gin"
	"github.com/namtph/go-redis-cron/ui"
)

// Mount registers the job dashboard and API on a Gin engine.
func Mount(r *gin.Engine, src ui.JobSource, opts ui.Options) {
	h := ui.Handler(src, opts)
	prefix := ui.URLPrefix(opts)
	r.Any(prefix, gin.WrapH(h))
	r.Any(prefix+"/*filepath", gin.WrapH(h))
}
