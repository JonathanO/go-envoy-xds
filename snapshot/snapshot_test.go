package snapshot_test

import (
	"maps"
	"slices"
	"testing"
	"time"

	clusterv3 "github.com/envoyproxy/go-control-plane/envoy/config/cluster/v3"
	endpointv3 "github.com/envoyproxy/go-control-plane/envoy/config/endpoint/v3"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/wrapperspb"

	xds "github.com/JonathanO/go-envoy-xds"
	"github.com/JonathanO/go-envoy-xds/snapshot"
)

func cluster(name string, timeoutSeconds int64) *clusterv3.Cluster {
	return &clusterv3.Cluster{Name: name, ConnectTimeout: durationpb.New(time.Duration(timeoutSeconds) * time.Second)}
}

func everyone(string) string { return "v" }

type src = *snapshot.Source[string, string, string]

// subscribe returns a Source's Envoy, and the updates it's delivered.
func subscribe(s src, details string) (*snapshot.Update, *[]*snapshot.Update, func()) {
	var got []*snapshot.Update
	current, cancel := s.Subscribe(details, func(u *snapshot.Update) { got = append(got, u) })
	return current, &got, cancel
}

var all = func(string) bool { return true }

func clusterNames(rs []xds.Resource) []string {
	var ns []string
	for _, r := range rs {
		ns = append(ns, r.Name)
	}
	slices.Sort(ns)
	return ns
}

func TestEditorRecordsOnlyRealChanges(t *testing.T) {
	a := cluster("a", 1)
	s := snapshot.Empty().Edit()
	s.Put(a)
	first := s.Build()
	e := first.Edit()
	e.Put(cluster("a", 1)) // equal
	e.Remove(xds.ClusterType, "missing")
	if got, _ := e.Build().Resource(xds.ClusterType, "a"); got != a {
		t.Fatalf("the equal put replaced a")
	}
	src := snapshot.New[string, string, string](everyone)
	src.Update(func(c *snapshot.Change[string, string]) { c.SetView("v", "g"); c.SetGroup("g", first) })
	_, got, _ := subscribe(src, "envoy")
	src.Update(func(c *snapshot.Change[string, string]) {
		c.EditGroup("g", func(e *snapshot.Editor) { e.Put(cluster("a", 1)) })
	})
	if len(*got) != 0 {
		t.Fatalf("an edit that changed nothing woke the view: %v", *got)
	}
}

func TestPutNamesEndpointsByCluster(t *testing.T) {
	e := snapshot.Empty().Edit()
	e.Put(&endpointv3.ClusterLoadAssignment{ClusterName: "a"})
	if _, ok := e.Build().Resource(xds.EndpointType, "a"); !ok {
		t.Fatal("no endpoints for a")
	}
}

func TestPutPanicsForAResourceWithNoName(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("no panic")
		}
	}()
	snapshot.Empty().Edit().Put(wrapperspb.String("x"))
}

func TestChangingAGroupWakesOnlyTheViewsWithIt(t *testing.T) {
	s := snapshot.New[string, string, string](func(d string) string { return d })
	s.Update(func(c *snapshot.Change[string, string]) {
		c.SetView("x", "shared", "x-only")
		c.SetView("y", "shared")
	})
	_, x, _ := subscribe(s, "x")
	_, y, _ := subscribe(s, "y")

	s.Update(func(c *snapshot.Change[string, string]) {
		c.EditGroup("x-only", func(e *snapshot.Editor) { e.Put(cluster("a", 1)) })
	})
	if len(*x) != 1 || len(*y) != 0 {
		t.Fatalf("x got %d, y got %d updates, want 1 and 0", len(*x), len(*y))
	}
}

func TestTheFirstGroupInAViewWins(t *testing.T) {
	s := snapshot.New[string, string, string](everyone)
	first, second := cluster("a", 1), cluster("a", 2)
	s.Update(func(c *snapshot.Change[string, string]) {
		c.SetView("v", "override", "base")
		c.EditGroup("base", func(e *snapshot.Editor) { e.Put(second) })
		c.EditGroup("override", func(e *snapshot.Editor) { e.Put(first) })
	})
	current, _, _ := subscribe(s, "envoy")
	b := snapshot.Builders[string](xds.ClusterType)[0]
	if rs := b.ResourcesRemoveOrder(current, all, ""); len(rs) != 1 || rs[0].Message != first {
		t.Fatalf("got %v, want the override", rs)
	}
}

func TestBuildersSendAddsInTheAddRoundAndRemovalsInTheRemoveRound(t *testing.T) {
	s := snapshot.New[string, string, string](everyone)
	s.Update(func(c *snapshot.Change[string, string]) {
		c.SetView("v", "g")
		c.EditGroup("g", func(e *snapshot.Editor) { e.Put(cluster("old", 1)); e.Put(cluster("same", 1)) })
	})
	_, got, _ := subscribe(s, "envoy")
	s.Update(func(c *snapshot.Change[string, string]) {
		c.EditGroup("g", func(e *snapshot.Editor) { e.Remove(xds.ClusterType, "old"); e.Put(cluster("new", 1)) })
	})
	u := (*got)[0]
	b := snapshot.Builders[string](xds.ClusterType)[0]

	if adds := b.AddOrder(u, all, ""); !slices.Equal(clusterNames(adds.Updates), []string{"new"}) || len(adds.Removes) != 0 {
		t.Errorf("add round: %v", adds)
	}
	if removes := b.RemoveOrder(u, all, ""); len(removes.Updates) != 0 || !slices.Equal(removes.Removes, []string{"old"}) {
		t.Errorf("remove round: %v", removes)
	}
	if mid := b.ResourcesAddOrder(u, all, ""); !slices.Equal(clusterNames(mid), []string{"new", "old", "same"}) {
		t.Errorf("mid-change: %v, want old still there", clusterNames(mid))
	}
	if end := b.ResourcesRemoveOrder(u, all, ""); !slices.Equal(clusterNames(end), []string{"new", "same"}) {
		t.Errorf("after: %v", clusterNames(end))
	}
	if adds := b.AddOrder(u, func(n string) bool { return n != "new" }, ""); len(adds.Updates) != 0 {
		t.Errorf("sent %v, which Envoy isn't subscribed to", adds.Updates)
	}
}

func TestUpdatesMergeAcrossChangesButNotAcrossPhases(t *testing.T) {
	s := snapshot.New[string, string, string](everyone)
	s.Update(func(c *snapshot.Change[string, string]) { c.SetView("v", "g") })
	_, got, _ := subscribe(s, "envoy")

	whole := s.Update(func(c *snapshot.Change[string, string]) {
		c.EditGroup("g", func(e *snapshot.Editor) { e.Put(cluster("a", 1)) })
		c.Then()
		c.Then() // an empty phase is no phase
		c.EditGroup("g", func(e *snapshot.Editor) { e.Put(cluster("b", 1)) })
	})
	if len(*got) != 2 || (*got)[0].MergesWithLater() || !(*got)[1].MergesWithLater() {
		t.Fatalf("got %d updates, want the first phase's ending it", len(*got))
	}
	if g := s.Group("g"); !slices.Equal(slices.Sorted(maps.Keys(maps.Collect(g.Resources(xds.ClusterType)))), []string{"a", "b"}) {
		t.Errorf("a later phase didn't edit on top of the earlier one")
	}
	b := snapshot.Builders[string](xds.ClusterType)[0]
	if adds := b.AddOrder(whole["v"], all, ""); !slices.Equal(clusterNames(adds.Updates), []string{"a", "b"}) {
		t.Errorf("the caller's update has %v, want every phase", clusterNames(adds.Updates))
	}
	if merged := (*got)[0].Merge((*got)[1]); !slices.Equal(clusterNames(b.AddOrder(merged, all, "").Updates), []string{"a", "b"}) {
		t.Errorf("merged updates lost a change")
	}
}

func TestAnEnvoyWithNoViewSeesNothingUntilItsViewIsAdded(t *testing.T) {
	s := snapshot.New[string, string, string](everyone)
	s.Update(func(c *snapshot.Change[string, string]) {
		c.EditGroup("g", func(e *snapshot.Editor) { e.Put(cluster("a", 1)) })
	})
	current, got, _ := subscribe(s, "envoy")
	b := snapshot.Builders[string](xds.ClusterType)[0]
	if rs := b.ResourcesRemoveOrder(current, all, ""); len(rs) != 0 {
		t.Fatalf("got %v with no view", rs)
	}

	s.Update(func(c *snapshot.Change[string, string]) { c.SetView("v", "g") })
	if len(*got) != 1 || !slices.Equal(clusterNames(b.AddOrder((*got)[0], all, "").Updates), []string{"a"}) {
		t.Fatalf("adding the view didn't send a")
	}
	s.Update(func(c *snapshot.Change[string, string]) { c.SetView("v") })
	if len(*got) != 2 || !slices.Equal(b.RemoveOrder((*got)[1], all, "").Removes, []string{"a"}) {
		t.Fatalf("removing the view didn't remove a")
	}
}

func TestRemovingAGroupRemovesWhatItHad(t *testing.T) {
	s := snapshot.New[string, string, string](everyone)
	s.Update(func(c *snapshot.Change[string, string]) {
		c.SetView("v", "g")
		c.EditGroup("g", func(e *snapshot.Editor) { e.Put(cluster("a", 1)) })
	})
	_, got, _ := subscribe(s, "envoy")
	s.Update(func(c *snapshot.Change[string, string]) { c.RemoveGroup("g") })
	b := snapshot.Builders[string](xds.ClusterType)[0]
	if len(*got) != 1 || !slices.Equal(b.RemoveOrder((*got)[0], all, "").Removes, []string{"a"}) {
		t.Fatalf("removing the group didn't remove a")
	}
	if _, ok := s.Group("g").Resource(xds.ClusterType, "a"); ok {
		t.Fatal("the group's still there")
	}
}

func TestCancelledEnvoysGetNoMoreUpdates(t *testing.T) {
	s := snapshot.New[string, string, string](everyone)
	s.Update(func(c *snapshot.Change[string, string]) { c.SetView("v", "g") })
	_, got, cancel := subscribe(s, "envoy")
	_, other, _ := subscribe(s, "other")
	cancel()
	s.Update(func(c *snapshot.Change[string, string]) {
		c.EditGroup("g", func(e *snapshot.Editor) { e.Put(cluster("a", 1)) })
	})
	if len(*got) != 0 || len(*other) != 1 {
		t.Fatalf("cancelled got %d, other got %d", len(*got), len(*other))
	}
}

func TestMergingUpdatesOutOfOrderPanics(t *testing.T) {
	s := snapshot.New[string, string, string](everyone)
	s.Update(func(c *snapshot.Change[string, string]) { c.SetView("v", "g") })
	_, got, _ := subscribe(s, "envoy")
	for range 2 {
		s.Update(func(c *snapshot.Change[string, string]) {
			c.EditGroup("g", func(e *snapshot.Editor) { e.Put(cluster("a", int64(len(*got)+1))) })
		})
	}
	defer func() {
		if recover() == nil {
			t.Fatal("no panic")
		}
	}()
	(*got)[1].Merge((*got)[0])
}

func TestSnapshotsAndSourcesSayWhatTheyHave(t *testing.T) {
	e := snapshot.Empty().Edit()
	e.Put(cluster("a", 1))
	e.Put(&endpointv3.ClusterLoadAssignment{ClusterName: "a"})
	if got := slices.Sorted(e.Build().Types()); !slices.Equal(got, []xds.TypeURL{xds.ClusterType, xds.EndpointType}) {
		t.Errorf("types %v", got)
	}
	s := snapshot.New[string, string, string](everyone)
	s.Update(func(c *snapshot.Change[string, string]) { c.SetView("v", "x", "y") })
	if got := s.View("v"); !slices.Equal(got, []string{"x", "y"}) {
		t.Errorf("view %v", got)
	}
}
