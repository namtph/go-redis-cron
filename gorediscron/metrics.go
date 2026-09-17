package gorediscron

// Metrics collects runtime counters (optional; defaults to no-op).
type Metrics interface {
	IncClaim()
	IncRequeue()
	IncAck()
	IncRedisOOM()
	SetLocalQueueDepth(n int)
	IncRegisterApplied()
	IncRegisterSkipped()
}

type nopMetrics struct{}

func (nopMetrics) IncClaim()              {}
func (nopMetrics) IncRequeue()            {}
func (nopMetrics) IncAck()                {}
func (nopMetrics) IncRedisOOM()           {}
func (nopMetrics) SetLocalQueueDepth(int) {}
func (nopMetrics) IncRegisterApplied()    {}
func (nopMetrics) IncRegisterSkipped()    {}

func metricsOrNop(m Metrics) Metrics {
	if m == nil {
		return nopMetrics{}
	}
	return m
}
