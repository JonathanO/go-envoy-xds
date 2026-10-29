package xds

import (
	"slices"
	"sync"
)

// A mailbox holds the updates a Source delivers to a stream until the stream is ready for them. Mesh updates arrive
// in bursts, and pushing each to every Envoy is expensive, so updates that are Mergers merge while they wait.
type mailbox[U any] struct {
	mu      sync.Mutex
	waiting []U
	ready   chan struct{} // has a value while there may be something waiting
}

func newMailbox[U any]() *mailbox[U] {
	return &mailbox[U]{ready: make(chan struct{}, 1)}
}

// put never blocks, so a Source can call it while holding its own lock.
func (m *mailbox[U]) put(u U) {
	m.mu.Lock()
	if n := len(m.waiting); n > 0 && mergesWithLater(m.waiting[n-1]) {
		m.waiting[n-1] = any(m.waiting[n-1]).(Merger[U]).Merge(u)
	} else {
		m.waiting = append(m.waiting, u)
	}
	m.mu.Unlock()
	select {
	case m.ready <- struct{}{}:
	default:
	}
}

func mergesWithLater[U any](u U) bool {
	if _, ok := any(u).(Merger[U]); !ok {
		return false
	}
	p, ok := any(u).(phaser)
	return !ok || p.MergesWithLater()
}

func (m *mailbox[U]) take() (u U, ok bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(m.waiting) == 0 {
		return
	}
	u = m.waiting[0]
	m.waiting = slices.Delete(m.waiting, 0, 1) // which zeroes the end, so what was there can be collected
	return u, true
}
