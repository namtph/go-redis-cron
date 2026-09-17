package ui

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/namtph/go-redis-cron/gorediscron"
)

type stubSource struct {
	jobs []gorediscron.Job
}

func (s stubSource) Jobs() []gorediscron.Job { return s.jobs }

func TestJobsAPI(t *testing.T) {
	src := stubSource{jobs: []gorediscron.Job{{
		ID: "x", Name: "Test", Spec: "* * * * *", LastStatus: gorediscron.RunStatusUnknown,
	}}}
	h := Handler(src, Options{Prefix: "/cron"})

	req := httptest.NewRequest(http.MethodGet, "/cron/api/jobs", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status %d", rec.Code)
	}

	var body jobsResponse
	if err := json.NewDecoder(rec.Body).Decode(&body); err != nil {
		t.Fatal(err)
	}
	if len(body.Jobs) != 1 || body.Jobs[0].ID != "x" {
		t.Fatalf("unexpected body: %+v", body)
	}
}
