package ui

import (
	"embed"
	"io/fs"
	"net/http"
	"strings"
)

//go:embed static/*
var staticFS embed.FS

// URLPrefix returns the normalized path prefix used by Handler.
func URLPrefix(opts Options) string {
	return normalizePrefix(opts.Prefix)
}

// Handler serves the dashboard and job API under Options.Prefix.
func Handler(src JobSource, opts Options) http.Handler {
	prefix := URLPrefix(opts)
	mux := http.NewServeMux()

	sub, err := fs.Sub(staticFS, "static")
	if err != nil {
		panic("ui: embed static: " + err.Error())
	}
	fileServer := http.FileServer(http.FS(sub))

	mux.HandleFunc(prefix+"/api/jobs", JobsHandler(src))
	mux.Handle(prefix+"/", http.StripPrefix(prefix, fileServer))
	mux.HandleFunc(prefix, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != prefix && r.URL.Path != prefix+"/" {
			http.NotFound(w, r)
			return
		}
		http.Redirect(w, r, prefix+"/", http.StatusFound)
	})

	return mux
}

func normalizePrefix(prefix string) string {
	if prefix == "" {
		prefix = "/cron"
	}
	if !strings.HasPrefix(prefix, "/") {
		prefix = "/" + prefix
	}
	return strings.TrimSuffix(prefix, "/")
}
