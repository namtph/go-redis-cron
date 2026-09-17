package gorediscron

// WorkerPoolConfig configures StartWorkerPool.
type WorkerPoolConfig struct {
	// NumberOfWorkerInstances is how many goroutines dequeue from the local queue.
	NumberOfWorkerInstances int
}

func (c WorkerPoolConfig) validate() error {
	if c.NumberOfWorkerInstances < 1 {
		return errInvalidWorkerPoolConfig
	}
	return nil
}
