package gorediscron

import (
	"sync"
	"time"

	"github.com/robfig/cron/v3"
)

type jobRecord struct {
	id       string
	name     string
	spec     string
	entryID  cron.EntryID
	nextRun  time.Time
	lastRun  time.Time
	lastErr  string
	status   RunStatus
}

type jobRegistry struct {
	mu    sync.RWMutex
	byID  map[string]*jobRecord
	order []string
}

func newJobRegistry() *jobRegistry {
	return &jobRegistry{byID: make(map[string]*jobRecord)}
}

func (r *jobRegistry) len() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.byID)
}

func (r *jobRegistry) add(id, name, spec string, entryID cron.EntryID, next time.Time) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, exists := r.byID[id]; !exists {
		r.order = append(r.order, id)
	}
	r.byID[id] = &jobRecord{
		id:      id,
		name:    name,
		spec:    spec,
		entryID: entryID,
		nextRun: next,
		status:  RunStatusUnknown,
	}
}

func (r *jobRegistry) markRun(id string, err error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	rec, ok := r.byID[id]
	if !ok {
		return
	}
	rec.lastRun = time.Now()
	if err != nil {
		rec.lastErr = err.Error()
		rec.status = RunStatusFailed
	} else {
		rec.lastErr = ""
		rec.status = RunStatusSuccess
	}
}

func (r *jobRegistry) markSkipped(id string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	rec, ok := r.byID[id]
	if !ok {
		return
	}
	rec.status = RunStatusSkipped
}

func (r *jobRegistry) snapshot(isLeader bool) []Job {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]Job, 0, len(r.order))
	for _, id := range r.order {
		rec := r.byID[id]
		out = append(out, Job{
			ID:         rec.id,
			Name:       rec.name,
			Spec:       rec.spec,
			NextRun:    rec.nextRun,
			LastRun:    rec.lastRun,
			LastError:  rec.lastErr,
			LastStatus: rec.status,
			LeaderRun:  isLeader,
		})
	}
	return out
}
