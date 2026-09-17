package internal

import "fmt"

// LeaderKey returns a Redis key with a hash tag for cluster-safe Lua scripts.
func LeaderKey(namespace string) string {
	return fmt.Sprintf("{%s}:leader", namespace)
}
