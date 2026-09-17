package internal

import "fmt"

func tag(namespace string) string {
	return "{" + namespace + "}"
}

// LeaderKey returns a Redis key with a hash tag for cluster-safe Lua scripts.
func LeaderKey(namespace string) string {
	return fmt.Sprintf("%s:leader", tag(namespace))
}

// RunQueueKey is the list of pending job runs.
func RunQueueKey(namespace string) string {
	return fmt.Sprintf("%s:runs:pending", tag(namespace))
}

// RunProcessingListKey holds runs claimed from pending but not yet acked.
func RunProcessingListKey(namespace string) string {
	return fmt.Sprintf("%s:runs:processing", tag(namespace))
}

// RunProcessingMetaKey maps raw task JSON to worker|deadlineMs.
func RunProcessingMetaKey(namespace string) string {
	return fmt.Sprintf("%s:runs:processing:meta", tag(namespace))
}

// RunLeaseKey is a ZSET of processing tasks by lease deadline (ms).
func RunLeaseKey(namespace string) string {
	return fmt.Sprintf("%s:runs:leases", tag(namespace))
}

// RunClaimKey deduplicates a scheduled tick across pods.
func RunClaimKey(namespace string, name string, scheduledAt int64) string {
	return fmt.Sprintf("%s:claim:%s:%d", tag(namespace), name, scheduledAt)
}

// ActiveRunKey marks an in-flight run when parallel execution is disabled.
func ActiveRunKey(namespace string, name string) string {
	return fmt.Sprintf("%s:active:%s", tag(namespace), name)
}

// SchedMetaKey stores scheduler metadata in Redis.
func SchedMetaKey(namespace string, name string) string {
	return fmt.Sprintf("%s:sched:%s", tag(namespace), name)
}
