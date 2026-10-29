package snapshot

import (
	"slices"
	"sync"

	xds "github.com/JonathanO/go-envoy-xds"
)

// A Source holds named groups of resources and named views of them, and tells each Envoy when its view changes.
//
// A group is a Snapshot: any resources, of any types. A view is an ordered list of groups, and an Envoy sees every
// resource in its view's groups; where two of them have a resource of the same type and name, the first group's
// wins. viewOf picks each Envoy's view from its details, so Envoys with the same view share it.
//
// Groups and views are named by keys of your choosing: strings, or types of your own, so a view's name can't be used
// as a group's by mistake. An Envoy whose view doesn't exist sees nothing until you add it.
type Source[D any, G, V comparable] struct {
	viewOf func(D) V

	mu           sync.Mutex
	groups       map[G]*Snapshot
	views        map[V][]G
	viewsByGroup map[G]map[V]struct{}
	states       map[V]*viewState // each view as its Envoys were last sent it
	subscribers  map[V]map[*subscriber]struct{}
}

type subscriber struct {
	deliver func(*Update)
}

// New returns an empty Source. viewOf names the view an Envoy sees, given its details.
func New[D any, G, V comparable](viewOf func(details D) V) *Source[D, G, V] {
	return &Source[D, G, V]{
		viewOf:       viewOf,
		groups:       map[G]*Snapshot{},
		views:        map[V][]G{},
		viewsByGroup: map[G]map[V]struct{}{},
		states:       map[V]*viewState{},
		subscribers:  map[V]map[*subscriber]struct{}{},
	}
}

// Group returns a group as last published, empty if there's none.
func (s *Source[D, G, V]) Group(name G) *Snapshot {
	s.mu.Lock()
	defer s.mu.Unlock()
	if g, ok := s.groups[name]; ok {
		return g
	}
	return empty
}

// View returns a view's groups as last published, none if there's no such view.
func (s *Source[D, G, V]) View(name V) []G {
	s.mu.Lock()
	defer s.mu.Unlock()
	return slices.Clone(s.views[name])
}

// Update makes any number of changes to groups and views, in edit, and publishes them together. Envoys get them as
// one change, so it's make before break: anything added is in place before anything referring to it, whatever groups
// they're in. To order changes make before break can't see, split them into phases with Change.Then.
//
// edit runs with the Source locked, so it mustn't call the Source. Update returns the update sent to each view that
// changed, with all its phases merged.
func (s *Source[D, G, V]) Update(edit func(c *Change[G, V])) map[V]*Update {
	s.mu.Lock()
	defer s.mu.Unlock()
	c := &Change[G, V]{base: s.groups, phases: []*phase[G, V]{newPhase[G, V]()}}
	edit(c)
	applied := make([]map[V]*Update, len(c.phases))
	for i, p := range c.phases {
		applied[i] = s.apply(p)
	}
	whole := map[V]*Update{}
	for i, updates := range applied {
		for view, u := range updates {
			sent := u
			// A view in a later phase gets this one on its own first; others merge as usual.
			if slices.ContainsFunc(applied[i+1:], func(later map[V]*Update) bool { _, ok := later[view]; return ok }) {
				ends := *u
				ends.endsPhase = true
				sent = &ends
			}
			if prev, ok := whole[view]; ok {
				whole[view] = prev.Merge(u)
			} else {
				whole[view] = u
			}
			for sub := range s.subscribers[view] {
				sub.deliver(sent)
			}
		}
	}
	return whole
}

// apply publishes one phase, and returns the update to each view it changed.
func (s *Source[D, G, V]) apply(p *phase[G, V]) map[V]*Update {
	changedViews := map[V]struct{}{}
	for name, groups := range p.views {
		if !slices.Equal(s.views[name], groups) {
			s.setView(name, groups)
			changedViews[name] = struct{}{}
		}
	}
	touchedByView := map[V][]map[xds.TypeURL]names{}
	for name, g := range p.groups {
		byType, touched := p.touched[name] // only if a name was
		switch {
		case g == nil:
			delete(s.groups, name)
		case touched:
			s.groups[name] = g
		}
		if touched {
			for v := range s.viewsByGroup[name] {
				touchedByView[v] = append(touchedByView[v], byType)
			}
		}
	}
	for v := range changedViews {
		if _, ok := touchedByView[v]; !ok {
			touchedByView[v] = nil
		}
	}
	updates := map[V]*Update{}
	for v, touched := range touchedByView {
		before := s.states[v]
		if before == nil {
			before = emptyView
		}
		after := s.stateOf(v)
		if after == emptyView {
			delete(s.states, v)
		} else {
			s.states[v] = after
		}
		_, full := changedViews[v]
		u := &Update{previous: before, current: after, full: full}
		if !full {
			u.touched = touched
		}
		updates[v] = u
	}
	return updates
}

func (s *Source[D, G, V]) setView(name V, groups []G) {
	for _, g := range s.views[name] {
		delete(s.viewsByGroup[g], name)
		if len(s.viewsByGroup[g]) == 0 {
			delete(s.viewsByGroup, g)
		}
	}
	for _, g := range groups {
		if s.viewsByGroup[g] == nil {
			s.viewsByGroup[g] = map[V]struct{}{}
		}
		s.viewsByGroup[g][name] = struct{}{}
	}
	if len(groups) == 0 {
		delete(s.views, name)
	} else {
		s.views[name] = groups
	}
}

func (s *Source[D, G, V]) stateOf(view V) *viewState {
	names, ok := s.views[view]
	if !ok {
		return emptyView
	}
	v := &viewState{groups: make([]*Snapshot, len(names))}
	for i, name := range names {
		v.groups[i] = empty
		if g, ok := s.groups[name]; ok {
			v.groups[i] = g
		}
	}
	return v
}

// Subscribe starts delivering the updates to an Envoy's view, for xds.Server.
func (s *Source[D, G, V]) Subscribe(details D, deliver func(*Update)) (*Update, func()) {
	view := s.viewOf(details)
	sub := &subscriber{deliver}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.subscribers[view] == nil {
		s.subscribers[view] = map[*subscriber]struct{}{}
	}
	s.subscribers[view][sub] = struct{}{}
	current := s.states[view]
	if current == nil {
		current = emptyView
	}
	return &Update{previous: emptyView, current: current, full: true}, func() {
		s.mu.Lock()
		defer s.mu.Unlock()
		delete(s.subscribers[view], sub)
		if len(s.subscribers[view]) == 0 {
			delete(s.subscribers, view)
		}
	}
}

// A Change is changes to groups and views, published together by Source.Update.
type Change[G, V comparable] struct {
	base   map[G]*Snapshot
	phases []*phase[G, V] // edits go to the last
}

type phase[G, V comparable] struct {
	groups  map[G]*Snapshot // as changed so far; nil means removed
	touched map[G]map[xds.TypeURL]names
	views   map[V][]G // as changed so far; none means removed
}

func newPhase[G, V comparable]() *phase[G, V] {
	return &phase[G, V]{groups: map[G]*Snapshot{}, touched: map[G]map[xds.TypeURL]names{}, views: map[V][]G{}}
}

func (p *phase[G, V]) touch(name G, t xds.TypeURL, ns names) {
	if p.touched[name] == nil {
		p.touched[name] = map[xds.TypeURL]names{}
	}
	if p.touched[name][t] == nil {
		p.touched[name][t] = names{}
	}
	for n := range ns {
		p.touched[name][t][n] = struct{}{}
	}
}

func (p *phase[G, V]) touchAll(name G, s *Snapshot) {
	for t, rs := range s.resources {
		ns := make(names, len(rs))
		for n := range rs {
			ns[n] = struct{}{}
		}
		p.touch(name, t, ns)
	}
}

func (c *Change[G, V]) last() *phase[G, V] { return c.phases[len(c.phases)-1] }

// Then starts the next phase. Each Envoy is sent everything before it, and ACKs it, before it's sent anything after,
// to order changes make before break can't see: a route that stops using a subset of a cluster, then the cluster
// dropping it, say. Each phase is still make before break within itself.
//
// Only Envoys whose view changes in a later phase wait. An Envoy connecting meanwhile gets the end result.
func (c *Change[G, V]) Then() {
	c.phases = append(c.phases, newPhase[G, V]()) // an empty one changes no view, so costs nothing
}

// EditGroup edits a group, creating it if there's none. Only the names edit touches are compared.
func (c *Change[G, V]) EditGroup(name G, edit func(e *Editor)) {
	e := c.Group(name).Edit()
	edit(e)
	p := c.last()
	for t, ns := range e.touched {
		p.touch(name, t, ns)
	}
	p.groups[name] = e.Build()
}

// SetGroup replaces a group with a snapshot made however you like. Every name in the old and new group is compared.
func (c *Change[G, V]) SetGroup(name G, s *Snapshot) {
	p := c.last()
	p.touchAll(name, c.Group(name))
	p.touchAll(name, s)
	p.groups[name] = s
}

// RemoveGroup removes a group. Views listing it see nothing of it until it's added again.
func (c *Change[G, V]) RemoveGroup(name G) {
	p := c.last()
	p.touchAll(name, c.Group(name))
	p.groups[name] = nil
}

// Group returns a group as this change has it so far, earlier phases included, empty if there's none.
func (c *Change[G, V]) Group(name G) *Snapshot {
	for _, p := range slices.Backward(c.phases) {
		if g, ok := p.groups[name]; ok {
			if g == nil {
				return empty
			}
			return g
		}
	}
	if g, ok := c.base[name]; ok {
		return g
	}
	return empty
}

// SetView sets the groups a view is made of, first first, creating it if there's none. With no groups, it removes the
// view: its Envoys see nothing until it's added again.
func (c *Change[G, V]) SetView(name V, groups ...G) {
	c.last().views[name] = slices.Clone(groups)
}
