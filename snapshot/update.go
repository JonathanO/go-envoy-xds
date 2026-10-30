package snapshot

import (
	"sync"

	"google.golang.org/protobuf/proto"

	xds "github.com/JonathanO/go-envoy-xds"
)

// A viewState is one view as published at one point: its groups, in order. Groups are immutable and shared, so it
// costs a pointer per group, and an Envoy holding it keeps nothing else alive.
type viewState struct {
	groups []*Snapshot

	mu sync.Mutex
	// What the view sees of each type, merged, once something needs the whole view: an answer to a subscription, or an
	// update that changed the view's groups. Changes to a few names look through the groups instead, so never pay to
	// build it, nor take the lock.
	merged map[xds.TypeURL]map[string]proto.Message
}

var emptyView = &viewState{}

// lookup returns what the view sees by that type and name: the first of its groups to have it.
func (v *viewState) lookup(t xds.TypeURL, name string) proto.Message {
	for _, g := range v.groups {
		if m, ok := g.Resource(t, name); ok {
			return m
		}
	}
	return nil
}

func (v *viewState) all(t xds.TypeURL) map[string]proto.Message {
	v.mu.Lock()
	defer v.mu.Unlock()
	if all, ok := v.merged[t]; ok {
		return all
	}
	all := map[string]proto.Message{}
	for _, g := range v.groups {
		for name, m := range g.Resources(t) {
			if _, ok := all[name]; !ok {
				all[name] = m
			}
		}
	}
	if v.merged == nil {
		v.merged = map[xds.TypeURL]map[string]proto.Message{}
	}
	v.merged[t] = all
	return all
}

// An Update moves one view from one state to the next. Every Envoy seeing the view shares it.
//
// Updates merge, keeping the oldest previous state and the newest current one, so any number arriving while an Envoy
// is busy become one change, except across the end of a phase: see Change.Then.
type Update struct {
	previous, current *viewState
	full              bool                    // the view's groups changed, so every name in it is compared
	touched           []map[xds.TypeURL]names // otherwise, the names touched in each changed group of the view
	endsPhase         bool                    // a later phase of the same change follows, so nothing merges into this
}

// Merge returns the update from u's previous state to next's current one. It panics if next doesn't follow on from u.
func (u *Update) Merge(next *Update) *Update {
	if next.previous != u.current {
		panic("snapshot: updates merged out of order")
	}
	m := &Update{previous: u.previous, current: next.current, full: u.full || next.full, endsPhase: next.endsPhase}
	if !m.full {
		m.touched = append(append(m.touched, u.touched...), next.touched...)
	}
	return m
}

// MergesWithLater reports whether later updates may merge into this one.
func (u *Update) MergesWithLater() bool { return !u.endsPhase }

func (u *Update) before(t xds.TypeURL, name string) proto.Message { return u.previous.lookup(t, name) }
func (u *Update) after(t xds.TypeURL, name string) proto.Message  { return u.current.lookup(t, name) }

// candidates are the names that may differ between previous and current.
func (u *Update) candidates(t xds.TypeURL) names {
	ns := names{}
	if u.full {
		for n := range u.previous.all(t) {
			ns[n] = struct{}{}
		}
		for n := range u.current.all(t) {
			ns[n] = struct{}{}
		}
		return ns
	}
	for _, byType := range u.touched {
		for n := range byType[t] {
			ns[n] = struct{}{}
		}
	}
	return ns
}
