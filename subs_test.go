package xds

import (
	"maps"
	"math/rand/v2"
	"slices"
	"testing"
)

// seed gives a fuzzer request sequences enough to explore every subscription change without fuzzing, and examples.
func seed(f *testing.F, examples ...[]byte) {
	for _, reqs := range examples {
		f.Add(reqs)
	}
	r := rand.New(rand.NewPCG(1, 2))
	for range 2000 {
		reqs := make([]byte, r.IntN(12))
		for i := range reqs {
			reqs[i] = byte(r.IntN(16))
			if r.IntN(4) == 0 {
				reqs[i] |= byte(r.IntN(16)) << 4 // for SotW, names listed again
			}
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
	// Nothing, twice; a name, then another; the wildcard, then with a name, twice; leaving it for its name, and for
	// nothing; a name, then nothing, twice.
	seed(f, []byte{0, 0}, []byte{1, 2}, []byte{8, 9, 9}, []byte{9, 1}, []byte{8, 0}, []byte{1, 0, 0})
	f.Fuzz(func(t *testing.T, reqs []byte) {
		for _, wild := range []bool{false, true} {
			var s sotwSubs = &sotwNames{}
			if wild {
				s = &sotwWildcard{}
			}
			sentNames, all, explicit := false, false, set{}
			for _, b := range reqs {
				ns := append(namesOf(b), namesOf(b>>4)...) // a repeated field: names may repeat
				if !wild {
					ns = withoutWildcard(ns)
				}
				before := subscribedTo(s)
				added, changed := s.update(ns)
				after := subscribedTo(s)
				checkAdded(t, before, after, added, changed)
				// A SotW request lists everything it's subscribed to.
				wasAll, was := all, explicit
				all = wild && (slices.Contains(ns, wildcard) || (len(ns) == 0 && !sentNames))
				sentNames = sentNames || len(ns) > 0
				explicit = setOf(withoutWildcard(ns))
				if want := all != wasAll || !maps.Equal(explicit, was); changed != want {
					t.Fatalf("%v: changed is %v", ns, changed) // every ACK repeats the last request
				}
				newly := maps.Clone(explicit)
				maps.DeleteFunc(newly, func(n string, _ struct{}) bool { return was.has(n) })
				switch {
				case all && !wasAll:
					for _, n := range resources {
						if added == nil || !added(n) {
							t.Fatalf("%v newly subscribed to everything, but %s is not in the answer", ns, n)
						}
					}
				case wasAll && !all && len(newly) > 0:
					// Leaving the wildcard, the names newly listed were subscribed to already, so need no answer,
					// though java-envoy-xds sends one.
				default:
					// The answer is the names newly listed, or nothing.
					if (added == nil) != (len(newly) == 0) {
						t.Fatalf("%v newly listed %v, but answered %v", ns, newly, added != nil)
					}
					for _, n := range resources {
						if added != nil && added(n) != newly.has(n) {
							t.Fatalf("%v newly listed %v: %s in the answer is %v", ns, newly, n, added(n))
						}
					}
				}
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
	// Pairs of subscribe and unsubscribe: a name, then more; names, then fewer; nothing; the wildcard alone; with a
	// name, after another; with a name, then another, then leaving it for a third, then back.
	seed(f, []byte{1, 0, 3, 0}, []byte{3, 0, 0, 1}, []byte{0, 0}, []byte{8, 0}, []byte{1, 0, 10, 0},
		[]byte{9, 0, 2, 0, 4, 8, 8, 0})
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
				wasAll := all
				switch {
				case wild && first && len(sub) == 0 && len(unsub) == 0:
					all = true
				case slices.Contains(sub, wildcard):
					all = true
				case slices.Contains(unsub, wildcard):
					all = false
				}
				first = false
				// Unsubscribing needs no answer, and an empty one would hold up pushes until Envoy ACKed it.
				if want := len(withoutWildcard(sub)) > 0 || all && !wasAll; changed != want {
					t.Fatalf("after +%v -%v: changed is %v", sub, unsub, changed)
				}
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
