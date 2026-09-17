package ui

import (
	"encoding/json"
	"net/http"
	"time"

	"github.com/namtph/go-redis-cron/gorediscron"
)

// JobSource supplies job snapshots for the dashboard API.
type JobSource interface {
	Jobs() []gorediscron.Job
}

// Options configures the UI handler.
type Options struct {
	// Prefix is the URL path prefix for static assets and API routes (default "/cron").
	Prefix string
}

type jobDTO struct {
	ID         string    `json:"id"`
	Name       string    `json:"name"`
	Spec       string    `json:"spec"`
	NextRun    time.Time `json:"nextRun"`
	LastRun    time.Time `json:"lastRun"`
	LastError  string    `json:"lastError"`
	LastStatus string    `json:"lastStatus"`
	LeaderRun  bool      `json:"leaderRun"`
}

type jobsResponse struct {
	Jobs []jobDTO `json:"jobs"`
}

func writeJobs(w http.ResponseWriter, src JobSource) {
	jobs := src.Jobs()
	out := jobsResponse{Jobs: make([]jobDTO, 0, len(jobs))}
	for _, j := range jobs {
		out.Jobs = append(out.Jobs, jobDTO{
			ID:         j.ID,
			Name:       j.Name,
			Spec:       j.Spec,
			NextRun:    j.NextRun,
			LastRun:    j.LastRun,
			LastError:  j.LastError,
			LastStatus: string(j.LastStatus),
			LeaderRun:  j.LeaderRun,
		})
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(out)
}

// JobsHandler returns JSON for GET /api/jobs under the configured prefix.
func JobsHandler(src JobSource) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		writeJobs(w, src)
	}
}
