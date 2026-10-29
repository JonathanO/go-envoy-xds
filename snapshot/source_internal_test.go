package snapshot

import (
	"fmt"
	"maps"
	"math/rand/v2"
	"reflect"
	"runtime"
	"slices"
	"sync"
	"testing"

	clusterv3 "github.com/envoyproxy/go-control-plane/envoy/config/cluster/v3"
	routev3 "github.com/envoyproxy/go-control-plane/envoy/config/route/v3"

	xds "github.com/JonathanO/go-envoy-xds"
)

func TestASourceForgetsWhatsGone(t *testing.T) {
	s := New[string, string, string](func(d string) string { return d })
	s.Update(func(c *Change[string, string]) {
		c.SetView("v", "g")
		c.EditGroup("g", func(e *Editor) { e.Put(&clusterv3.Cluster{Name: "a"}) })
	})
	_, cancel := s.Subscribe("v", func(*Update) {})
	cancel()
	s.Update(func(c *Change[string, string]) {
		c.SetView("v")
		c.RemoveGroup("g")
	})
	if len(s.groups) != 0 || len(s.views) != 0 || len(s.viewsByGroup) != 0 || len(s.states) != 0 || len(s.subscribers) != 0 {
		t.Fatalf("left groups %v, views %v, viewsByGroup %v, states %v, subscribers %v", s.groups, s.views, s.viewsByGroup, s.states, s.subscribers)
	}
}

func TestAViewMergesEachTypeOnce(t *testing.T) {
	e := Empty().Edit()
	e.Put(&clusterv3.Cluster{Name: "a"})
	v := &viewState{groups: []*Snapshot{e.Build(), empty}}
	v.all(xds.ClusterType)
	if allocs := testing.AllocsPerRun(10, func() { v.all(xds.ClusterType) }); allocs != 0 {
		t.Fatalf("%v allocations, want the merge reused", allocs)
	}
}

func TestEditingSharesUntouchedTypesAndRecordsChangedNames(t *testing.T) {
	e := Empty().Edit()
	e.Put(&clusterv3.Cluster{Name: "foo"})
	e.Put(&routev3.RouteConfiguration{Name: "r"})
	base := e.Build()
	e = base.Edit()
	e.Put(&clusterv3.Cluster{Name: "bar"})
	next := e.Build()
	if reflect.ValueOf(next.resources[xds.RouteType]).UnsafePointer() != reflect.ValueOf(base.resources[xds.RouteType]).UnsafePointer() {
		t.Error("routes weren't shared")
	}
	if len(e.touched) != 1 || !maps.Equal(e.touched[xds.ClusterType], names{"bar": {}}) {
		t.Errorf("touched %v, want bar alone", e.touched)
	}
	if len(base.resources[xds.ClusterType]) != 1 || len(next.resources[xds.ClusterType]) != 2 {
		t.Errorf("clusters %v, then %v", base.resources[xds.ClusterType], next.resources[xds.ClusterType])
	}
}

func TestEachUpdateFollowsOnFromWhatTheEnvoyHas(t *testing.T) {
	s := New[string, string, string](func(d string) string { return d })
	s.Update(func(c *Change[string, string]) { c.SetView("v", "g") })
	var got []*Update
	connected, _ := s.Subscribe("v", func(u *Update) { got = append(got, u) })
	for _, name := range []string{"a", "b"} {
		s.Update(func(c *Change[string, string]) {
			c.EditGroup("g", func(e *Editor) { e.Put(&clusterv3.Cluster{Name: name}) })
		})
	}
	if len(got) != 2 || got[0].previous != connected.current || got[1].previous != got[0].current || got[1].current.groups[0] != s.Group("g") {
		t.Fatalf("got %v, want each following on from the last", got)
	}
}

// A follower is an Envoy following a view, as a stream's mailbox does.
type follower struct {
	first  *Update
	cancel func()
	mu     sync.Mutex
	got    []*Update
}

func follow(s *Source[string, string, string]) *follower {
	f := &follower{}
	f.first, f.cancel = s.Subscribe("envoy", func(u *Update) {
		f.mu.Lock()
		defer f.mu.Unlock()
		f.got = append(f.got, u)
	})
	return f
}

// held returns the state the updates end at, if each followed on from the last.
func (f *follower) held() (*viewState, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	held := f.first.current
	for _, u := range f.got {
		if u.previous != held {
			return nil, false
		}
		held = u.current
	}
	return held, true
}

func oneGroup() *Source[string, string, string] {
	s := New[string, string, string](func(string) string { return "all" })
	s.Update(func(c *Change[string, string]) { c.SetView("all", "all") })
	return s
}

func TestEnvoysComingAndGoingWhileChangesArriveSeeEveryChangeInOrder(t *testing.T) {
	s := oneGroup()
	var (
		wg        sync.WaitGroup
		mu        sync.Mutex
		connected []*follower
	)
	for i := range 4 {
		wg.Go(func() {
			r := rand.New(rand.NewPCG(uint64(i), 0))
			var mine []*follower
			for n := range 200 {
				switch r.IntN(5) {
				case 0:
					mine = append(mine, follow(s))
				case 1:
					if len(mine) > 0 {
						j := r.IntN(len(mine))
						mine[j].cancel()
						if _, ok := mine[j].held(); !ok {
							t.Error("got an update from a state it didn't hold")
						}
						mine = slices.Delete(mine, j, j+1)
					}
				default:
					c := &clusterv3.Cluster{Name: []string{"a", "b", "c"}[r.IntN(3)], AltStatName: fmt.Sprint(i, n)}
					s.Update(func(ch *Change[string, string]) { ch.EditGroup("all", func(e *Editor) { e.Put(c) }) })
				}
			}
			mu.Lock()
			defer mu.Unlock()
			connected = append(connected, mine...)
		})
	}
	wg.Wait()
	for _, f := range connected {
		if held, ok := f.held(); !ok || held.groups[0] != s.Group("all") {
			t.Fatal("an Envoy didn't catch up in order")
		}
	}
}

func TestThousandsOfEnvoysNeedNoGoroutinesOfTheirOwn(t *testing.T) {
	s := oneGroup()
	before := runtime.NumGoroutine()
	envoys := make([]*follower, 5000)
	for i := range envoys {
		envoys[i] = follow(s)
	}
	for i := range 10 {
		s.Update(func(c *Change[string, string]) {
			c.EditGroup("all", func(e *Editor) { e.Put(&clusterv3.Cluster{Name: fmt.Sprint("cluster-", i)}) })
		})
	}
	if after := runtime.NumGoroutine(); after > before { // others, e.g. earlier tests', may have ended meanwhile
		t.Errorf("%d goroutines, then %d", before, after)
	}
	for _, f := range envoys {
		if held, ok := f.held(); !ok || held.groups[0] != s.Group("all") {
			t.Fatal("an Envoy didn't catch up in order")
		}
	}
}
