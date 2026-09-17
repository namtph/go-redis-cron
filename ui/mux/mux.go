package uimux

import (
	"net/http"

	"github.com/gorilla/mux"
	"github.com/namtph/go-redis-cron/ui"
)

// Mount registers the job dashboard and API on a gorilla/mux router.
func Mount(r *mux.Router, src ui.JobSource, opts ui.Options) {
	h := ui.Handler(src, opts)
	prefix := ui.URLPrefix(opts)
	r.PathPrefix(prefix).Handler(h)
}

// Handler returns a standalone mux.Router with only UI routes.
func Handler(src ui.JobSource, opts ui.Options) http.Handler {
	return ui.Handler(src, opts)
}
