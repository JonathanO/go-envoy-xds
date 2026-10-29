package xds

import (
	"maps"
	"math/rand/v2"
	"slices"
	"testing"
)

// seed gives a fuzzer request sequences enough to explore every subscription change without fuzzing.
func seed(f *testing.F) {
	r := rand.New(rand.NewPCG(1, 2))
	for range 2000 {
		reqs := make([]byte, r.IntN(12))
		for i := range reqs {
			reqs[i] = byte(r.IntN(16))
		}
		f.Add(reqs)
	}
}

// names decodes a fuzzer's byte as a subset of a few names, the wildcard among them.
func namesOf(b byte) []string {
	var ns []string
	for i, n := range []string{"a", "b", "c", wildcard} {
		if b&(1<<i) != 0 {
			ns = append(ns, n)
		}
	}
	return ns
}

var resources = []string{"a", "b", "c", "d"}

func subscribedTo(s interface{ subscribed(string) bool }) set {
	got := set{}
	for _, n := range resources {
		if s.subscribed(n) {
			got[n] = struct{}{}
		}
	}
	return got
}

// checkAdded checks an update's answer covers what it newly subscribed to, and only what's subscribed.
func checkAdded(t *testing.T, before, after set, added func(string) bool, changed bool) {
	t.Helper()
	for _, n := range resources {
		isAdded := added != nil && added(n)
		if _, was := before[n]; !was && after.has(n) && !isAdded {
			t.Fatalf("newly subscribed to %s, but it's not in the answer", n)
		}
		if isAdded && !after.has(n) {
			t.Fatalf("%s is in the answer, but not subscribed to", n)
		}
		if isAdded && !changed {
			t.Fatalf("%s is in the answer of an update that changed nothing", n)
		}
	}
}

func FuzzSotwSubs(f *testing.F) {
	seed(f)
	f.Fuzz(func(t *testing.T, reqs []byte) {
		for _, wild := range []bool{false, true} {
			var s sotwSubs = &sotwNames{}
			if wild {
				s = &sotwWildcard{}
			}
			sentNames := false
			var last []string
			for i, b := range reqs {
				ns := namesOf(b)
				if !wild {
					ns = withoutWildcard(ns)
				}
				before := subscribedTo(s)
				added, changed := s.update(ns)
				after := subscribedTo(s)
				checkAdded(t, before, after, added, changed)
				if !maps.Equal(before, after) && !changed {
					t.Fatalf("%v changed subscriptions from %v to %v, unchanged", ns, before, after)
				}
				if i > 0 && slices.Equal(ns, last) && changed {
					t.Fatalf("repeating %v changed something", ns) // as every ACK does
				}
				last = ns
				// A SotW request lists everything it's subscribed to.
				all := wild && (slices.Contains(ns, wildcard) || (len(ns) == 0 && !sentNames))
				sentNames = sentNames || len(ns) > 0
				for _, n := range resources {
					if want := all || slices.Contains(ns, n); after.has(n) != want {
						t.Fatalf("after %v: subscribed to %s is %v", ns, n, after.has(n))
					}
				}
			}
		}
	})
}

func FuzzDeltaSubs(f *testing.F) {
	seed(f)
	f.Fuzz(func(t *testing.T, reqs []byte) {
		for _, wild := range []bool{false, true} {
			var s deltaSubs = &deltaNames{}
			if wild {
				s = &deltaWildcard{}
			}
			explicit, all, first := set{}, false, true
			for i := 0; i+1 < len(reqs); i += 2 {
				sub, unsub := namesOf(reqs[i]), namesOf(reqs[i+1]&^reqs[i]) // never both at once
				if !wild {
					sub, unsub = withoutWildcard(sub), withoutWildcard(unsub)
				}
				before := subscribedTo(s)
				added, changed, err := s.update(sub, unsub)
				if err != nil {
					t.Fatal(err)
				}
				after := subscribedTo(s)
				checkAdded(t, before, after, added, changed)
				if s.subscribed(wildcard) || added != nil && added(wildcard) {
					t.Fatal("the wildcard is a resource")
				}
				for _, n := range resources {
					if added != nil && added(n) && before.has(n) && !slices.Contains(sub, n) && !slices.Contains(unsub, n) {
						t.Fatalf("%s is in the answer, but this request didn't name it, and it was already subscribed to", n)
					}
				}
				for _, n := range withoutWildcard(sub) {
					if added == nil || !added(n) {
						t.Fatalf("subscribed to %s, but it's not in the answer", n) // even if it already was, by wildcard
					}
				}
				// The spec's model: names subscribed to explicitly, and the wildcard, which a first request naming
				// nothing also means.
				for _, n := range withoutWildcard(sub) {
					explicit[n] = struct{}{}
				}
				for _, n := range withoutWildcard(unsub) {
					delete(explicit, n)
				}
				switch {
				case wild && first && len(sub) == 0 && len(unsub) == 0:
					all = true
				case slices.Contains(sub, wildcard):
					all = true
				case slices.Contains(unsub, wildcard):
					all = false
				}
				first = false
				for _, n := range resources {
					if want := all || explicit.has(n); after.has(n) != want {
						t.Fatalf("after +%v -%v: subscribed to %s is %v", sub, unsub, n, after.has(n))
					}
				}
			}
		}
	})
}

func TestDeltaSubscribingToAndUnsubscribingFromOneNameIsAnError(t *testing.T) {
	for _, s := range []deltaSubs{&deltaNames{}, &deltaWildcard{}} {
		if added, changed, err := s.update([]string{"a"}, []string{"a"}); err == nil || added != nil || changed {
			t.Fatalf("%T: got %v, %v, want an error", s, added != nil, changed)
		}
	}
}

func TestOnlyListenersClustersScopedRoutesAndVirtualHostsAreWildcard(t *testing.T) {
	for _, typ := range addOrder {
		want := typ == ListenerType || typ == ClusterType || typ == ScopedRouteType || typ == VirtualHostType
		if typ.Wildcard() != want {
			t.Errorf("%s: wildcard is %v", typ, !want)
		}
	}
}
