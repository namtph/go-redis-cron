package gorediscron

import (
	"crypto/sha256"
	"encoding/hex"
	"regexp"
	"strings"
)

const jobIDMaxLen = 120

var nonJobIDChar = regexp.MustCompile(`[^a-z0-9]+`)

// sanitizeJobIDPart normalizes one segment of a job definition id.
func sanitizeJobIDPart(s string) string {
	s = strings.ToLower(strings.TrimSpace(s))
	s = nonJobIDChar.ReplaceAllString(s, "_")
	s = strings.Trim(s, "_")
	for strings.Contains(s, "__") {
		s = strings.ReplaceAll(s, "__", "_")
	}
	if s == "" {
		return "empty"
	}
	if len(s) <= 64 {
		return s
	}
	sum := sha256.Sum256([]byte(s))
	return hex.EncodeToString(sum[:8])
}

// CronJobID returns a stable definition id for a cron job from task name and cron spec.
// Format: job_cronjob_{task}_{cron}. Used to detect noop re-register vs overwrite.
func CronJobID(taskName, cronSpec string) string {
	id := "job_cronjob_" + sanitizeJobIDPart(taskName) + "_" + sanitizeJobIDPart(cronSpec)
	if len(id) <= jobIDMaxLen {
		return id
	}
	sum := sha256.Sum256([]byte(id))
	return "job_cronjob_" + hex.EncodeToString(sum[:16])
}
