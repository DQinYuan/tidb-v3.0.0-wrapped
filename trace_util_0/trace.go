package trace_util_0

import "sync"

type Cover struct {
	mu    sync.RWMutex
	Trace map[[2]int]struct{}
	Pos   []int
}

type CoverCounter struct {
	sql string
	m map[string]*Cover
}
