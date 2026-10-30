package xds

import (
	"fmt"
	"maps"
	"slices"
)

// Subscription tracking, per stream and type. See
// https://www.envoyproxy.io/docs/envoy/latest/api-docs/xds_protocol#how-the-client-specifies-what-resources-to-return
//
// Each update reports what a request newly subscribed to, as added, which is nil if nothing was. changed is false if
// the request changed nothing.

const wildcard = "*"

type set map[string]struct{}

func setOf(names []string) set {
	s := make(set, len(names))
	for _, n := range names {
		s[n] = struct{}{}
	}
	return s
}

func (s set) has(name string) bool {
	_, ok := s[name]
	return ok
}

func notWildcard(name string) bool { return name != wildcard }

func withoutWildcard(names []string) []string {
	return slices.DeleteFunc(slices.Clone(names), func(n string) bool { return n == wildcard })
}

type sotwSubs interface {
	// update takes the names a SotW request lists: every name it's subscribed to.
	update(names []string) (added func(string) bool, changed bool)
	subscribed(name string) bool
}

type sotwNames struct {
	subs set
}

func (s *sotwNames) update(names []string) (func(string) bool, bool) {
	next := setOf(names)
	if maps.Equal(next, s.subs) {
		return nil, false
	}
	added := set{}
	for n := range next {
		if !s.subs.has(n) {
			added[n] = struct{}{}
		}
	}
	s.subs = next
	if len(added) == 0 {
		return nil, true
	}
	return added.has, true
}

func (s *sotwNames) subscribed(name string) bool { return s.subs.has(name) }

// sotwWildcard is for a type Envoy may subscribe to all of. It may list names too, which take effect once it leaves
// the wildcard.
type sotwWildcard struct {
	all       bool
	sentNames bool
	names     sotwNames
}

func (w *sotwWildcard) update(names []string) (func(string) bool, bool) {
	// An empty list is the legacy wildcard, unless the client has ever sent names: then it unsubscribes from everything.
	all := slices.Contains(names, wildcard) || (len(names) == 0 && !w.sentNames)
	w.sentNames = w.sentNames || len(names) > 0
	added, changed := w.names.update(withoutWildcard(names))
	was := w.all
	w.all = all
	switch {
	case all && !was:
		return func(string) bool { return true }, true
	case was && !all:
		return added, true // the caller drops what's no longer subscribed, and answers with the rest
	}
	return added, changed
}

func (w *sotwWildcard) subscribed(name string) bool { return w.all || w.names.subscribed(name) }

type deltaSubs interface {
	// update takes the names a delta request subscribes to and unsubscribes from.
	update(subscribe, unsubscribe []string) (added func(string) bool, changed bool, err error)
	subscribed(name string) bool
}

type deltaNames struct {
	subs set
}

func (s *deltaNames) update(subscribe, unsubscribe []string) (func(string) bool, bool, error) {
	drop := setOf(unsubscribe)
	for _, n := range subscribe {
		// The spec doesn't say which should win, and either could be wrong.
		if drop.has(n) {
			return nil, false, fmt.Errorf("subscribed to and unsubscribed from %q in one request", n)
		}
	}
	if s.subs == nil {
		s.subs = set{}
	}
	added := setOf(subscribe)
	maps.Copy(s.subs, added)
	for n := range drop {
		delete(s.subs, n)
	}
	if len(subscribe) == 0 {
		// Unsubscribing needs no answer, and an empty one would hold up pushes until Envoy ACKed it.
		return nil, false, nil
	}
	return added.has, true, nil
}

func (s *deltaNames) subscribed(name string) bool { return s.subs.has(name) }

// deltaWildcard is for a type Envoy may subscribe to all of. Names it subscribes to explicitly are kept too, as
// leaving the wildcard leaves it subscribed to them.
type deltaWildcard struct {
	started bool
	all     bool
	names   deltaNames
}

func (w *deltaWildcard) update(subscribe, unsubscribe []string) (func(string) bool, bool, error) {
	if !w.started {
		w.started = true
		// The legacy wildcard: a first request naming nothing.
		if len(subscribe) == 0 && len(unsubscribe) == 0 {
			w.all = true
			return notWildcard, true, nil
		}
	}
	was := w.all
	if slices.Contains(subscribe, wildcard) {
		w.all = true
		subscribe = withoutWildcard(subscribe)
	}
	if slices.Contains(unsubscribe, wildcard) {
		w.all = false // unsubscribing wins, and the names leave it be: none is the wildcard
	}
	newlyAll := w.all && !was
	added, changed, err := w.names.update(subscribe, unsubscribe)
	if err != nil {
		return nil, false, err
	}
	if !newlyAll {
		// Names added while subscribed to all still need an answer: the spec requires one, removing those that don't
		// exist.
		return added, changed, nil
	}
	// Newly subscribed to everything not subscribed to by name, and to whatever this request named.
	return func(n string) bool {
		return n != wildcard && (!w.names.subscribed(n) || (added != nil && added(n)))
	}, true, nil
}

func (w *deltaWildcard) subscribed(name string) bool {
	if w.all {
		return name != wildcard
	}
	return w.names.subscribed(name)
}
