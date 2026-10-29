package snapshot

import (
	"testing"

	clusterv3 "github.com/envoyproxy/go-control-plane/envoy/config/cluster/v3"

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
