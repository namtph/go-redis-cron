package gorediscron

import "testing"

func TestCronJobID_stableAndSanitized(t *testing.T) {
	a := CronJobID("billing.Sync", "0 * * * *")
	b := CronJobID("billing.Sync", "0 * * * *")
	if a != b {
		t.Fatalf("expected stable id, got %q vs %q", a, b)
	}
	if a != CronJobID("billing_sync", "0 * * * *") {
		t.Fatalf("expected equivalent sanitization, got %q", a)
	}
}

func TestCronJobID_changesWhenCronChanges(t *testing.T) {
	a := CronJobID("heartbeat", "*/10 * * * * *")
	b := CronJobID("heartbeat", "*/30 * * * * *")
	if a == b {
		t.Fatal("expected different ids for different cron specs")
	}
}
