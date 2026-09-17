package gorediscron

import (
	"context"
	"sync"
	"time"

	"github.com/robfig/cron/v3"
)

type schedRecord struct {
	kind          JobKind
	name          string
	taskName      string
	jobID         string
	args          []any
	cron          string
	version       int64
	allowParallel bool
	timeout       time.Duration
	stopped       bool
	fn            JobFunc // legacy Register only
	entryID       cron.EntryID
	nextRun       time.Time
	lastRun       time.Time
	lastErr       string
	lastStatus    RunStatus

	runMu     sync.Mutex
	runCancel context.CancelFunc
}

func (rec *schedRecord) cancelInFlight() {
	rec.runMu.Lock()
	defer rec.runMu.Unlock()
	if rec.runCancel != nil {
		rec.runCancel()
		rec.runCancel = nil
	}
}

func (rec *schedRecord) setRunCancel(cancel context.CancelFunc) {
	rec.runMu.Lock()
	defer rec.runMu.Unlock()
	rec.runCancel = cancel
}

func (rec *schedRecord) clearRunCancel() {
	rec.runMu.Lock()
	defer rec.runMu.Unlock()
	rec.runCancel = nil
}

type schedulerRegistry struct {
	mu     sync.RWMutex
	byName map[string]*schedRecord
	order  []string
}

func newSchedulerRegistry() *schedulerRegistry {
	return &schedulerRegistry{byName: make(map[string]*schedRecord)}
}

func (r *schedulerRegistry) len() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.byName)
}

func (r *schedulerRegistry) get(name string) (*schedRecord, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	rec, ok := r.byName[name]
	return rec, ok
}

func (r *schedulerRegistry) upsertJob(job CronJob, jobID string, version int64, entryID cron.EntryID, next time.Time) *schedRecord {
	r.mu.Lock()
	defer r.mu.Unlock()

	rec, exists := r.byName[job.Name]
	if exists {
		rec.cancelInFlight()
	} else {
		rec = &schedRecord{name: job.Name}
		r.byName[job.Name] = rec
		r.order = append(r.order, job.Name)
	}

	rec.kind = JobKindRepeat
	rec.taskName = job.TaskName
	rec.jobID = jobID
	rec.args = job.Args
	rec.cron = job.Cron
	rec.version = version
	rec.allowParallel = job.AllowParallel
	rec.timeout = job.runTimeout()
	rec.stopped = false
	rec.entryID = entryID
	rec.nextRun = next
	return rec
}

func (r *schedulerRegistry) upsertLegacy(def CronJobScheduler, version int64, entryID cron.EntryID, next time.Time) *schedRecord {
	r.mu.Lock()
	defer r.mu.Unlock()

	rec, exists := r.byName[def.Name]
	if exists {
		rec.cancelInFlight()
	} else {
		rec = &schedRecord{name: def.Name}
		r.byName[def.Name] = rec
		r.order = append(r.order, def.Name)
	}

	rec.kind = JobKindRepeat
	rec.taskName = def.Name
	rec.jobID = CronJobID(def.Name, def.Cron)
	rec.cron = def.Cron
	rec.version = version
	rec.allowParallel = def.AllowParallel
	rec.timeout = def.runTimeout()
	rec.stopped = false
	rec.fn = def.Fn
	rec.entryID = entryID
	rec.nextRun = next
	return rec
}

func (r *schedulerRegistry) stop(name string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	rec, ok := r.byName[name]
	if !ok {
		return false
	}
	rec.stopped = true
	rec.cancelInFlight()
	return true
}

func (r *schedulerRegistry) removeEntryID(name string) cron.EntryID {
	r.mu.Lock()
	defer r.mu.Unlock()
	rec, ok := r.byName[name]
	if !ok {
		return 0
	}
	id := rec.entryID
	rec.entryID = 0
	return id
}

func (r *schedulerRegistry) setNextRun(name string, next time.Time) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if rec, ok := r.byName[name]; ok {
		rec.nextRun = next
	}
}

func (r *schedulerRegistry) markRun(name string, status RunStatus, err error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	rec, ok := r.byName[name]
	if !ok {
		return
	}
	rec.lastRun = time.Now()
	rec.lastStatus = status
	if err != nil {
		rec.lastErr = err.Error()
	} else {
		rec.lastErr = ""
	}
}

func (r *schedulerRegistry) setEntryID(name string, id cron.EntryID) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if rec, ok := r.byName[name]; ok {
		rec.entryID = id
	}
}

func (r *schedulerRegistry) clearCronEntryIDs() {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, rec := range r.byName {
		rec.entryID = 0
	}
}

func (r *schedulerRegistry) activeCronJobs() []struct {
	name string
	cron string
} {
	r.mu.RLock()
	defer r.mu.RUnlock()
	var out []struct {
		name string
		cron string
	}
	for _, name := range r.order {
		rec := r.byName[name]
		if rec.stopped {
			continue
		}
		out = append(out, struct {
			name string
			cron string
		}{name: rec.name, cron: rec.cron})
	}
	return out
}

func (r *schedulerRegistry) snapshot(isLeader bool) []Job {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]Job, 0, len(r.order))
	for _, name := range r.order {
		rec := r.byName[name]
		out = append(out, Job{
			ID:            rec.name,
			Name:          rec.name,
			Kind:          rec.kind,
			Spec:          rec.cron,
			Version:       rec.version,
			AllowParallel: rec.allowParallel,
			Stopped:       rec.stopped,
			NextRun:       rec.nextRun,
			LastRun:       rec.lastRun,
			LastError:     rec.lastErr,
			LastStatus:    rec.lastStatus,
			LeaderRun:     isLeader,
		})
	}
	return out
}
