package snapshot_test

import (
	"fmt"
	"maps"
	"slices"
	"strings"
	"time"

	clusterv3 "github.com/envoyproxy/go-control-plane/envoy/config/cluster/v3"
	routev3 "github.com/envoyproxy/go-control-plane/envoy/config/route/v3"
	"google.golang.org/protobuf/types/known/durationpb"

	xds "github.com/JonathanO/go-envoy-xds"
	"github.com/JonathanO/go-envoy-xds/snapshot"
)

// Two teams' Envoys share a group. Each Update returns the update it sent to each view that changed, so it shows
// which Envoys a change wakes.
func ExampleSource_Update() {
	// An Envoy's view is the team in its node ID, e.g. "team-a/envoy-1".
	src := snapshot.New[string, string, string](func(nodeID string) string {
		team, _, _ := strings.Cut(nodeID, "/")
		return team
	})
	woken := func(updates map[string]*snapshot.Update) { fmt.Println(slices.Sorted(maps.Keys(updates))) }

	woken(src.Update(func(c *snapshot.Change[string, string]) {
		c.EditGroup("shared", func(e *snapshot.Editor) { e.Put(&clusterv3.Cluster{Name: "auth"}) })
		c.EditGroup("team-a", func(e *snapshot.Editor) { e.Put(&clusterv3.Cluster{Name: "a"}) })
		c.EditGroup("team-b", func(e *snapshot.Editor) { e.Put(&clusterv3.Cluster{Name: "b"}) })
		// A team's own group comes first, so it can override what's shared.
		c.SetView("team-a", "team-a", "shared")
		c.SetView("team-b", "team-b", "shared")
	}))

	// Only team-a's Envoys are woken, and they compare only cluster a.
	woken(src.Update(func(c *snapshot.Change[string, string]) {
		c.EditGroup("team-a", func(e *snapshot.Editor) {
			e.Put(&clusterv3.Cluster{Name: "a", ConnectTimeout: durationpb.New(time.Second)})
		})
	}))

	// Both teams see the shared group.
	woken(src.Update(func(c *snapshot.Change[string, string]) {
		c.EditGroup("shared", func(e *snapshot.Editor) { e.Put(&clusterv3.Cluster{Name: "metrics"}) })
	}))

	// Putting what's there already changes nothing, so wakes nobody.
	woken(src.Update(func(c *snapshot.Change[string, string]) {
		c.EditGroup("shared", func(e *snapshot.Editor) { e.Put(&clusterv3.Cluster{Name: "metrics"}) })
	}))
	fmt.Println(src.View("team-a"))
	// Output:
	// [team-a team-b]
	// [team-a]
	// [team-a team-b]
	// []
	// [team-a shared]
}

// Phases order changes that make before break can't see. Here a route moves from one cluster to another in one phase,
// and the old cluster goes in the next, so each Envoy is sent, and ACKs, the route before it loses the cluster.
func ExampleChange_Then() {
	routeTo := func(cluster string) *routev3.RouteConfiguration {
		return &routev3.RouteConfiguration{Name: "local", VirtualHosts: []*routev3.VirtualHost{{
			Name: "all", Domains: []string{"*"},
			Routes: []*routev3.Route{{
				Match:  &routev3.RouteMatch{PathSpecifier: &routev3.RouteMatch_Prefix{Prefix: "/"}},
				Action: &routev3.Route_Route{Route: &routev3.RouteAction{ClusterSpecifier: &routev3.RouteAction_Cluster{Cluster: cluster}}},
			}},
		}}}
	}
	src := snapshot.New[string, string, string](func(string) string { return "all" })
	src.Update(func(c *snapshot.Change[string, string]) {
		c.SetView("all", "routes", "clusters")
		c.EditGroup("routes", func(e *snapshot.Editor) { e.Put(routeTo("old")) })
		c.EditGroup("clusters", func(e *snapshot.Editor) {
			e.Put(&clusterv3.Cluster{Name: "old"})
			e.Put(&clusterv3.Cluster{Name: "new"})
		})
	})

	// A Server subscribes like this for each Envoy. An update that doesn't merge with later ones is sent, and ACKed,
	// before the next.
	_, cancel := src.Subscribe("envoy-1", func(u *snapshot.Update) {
		fmt.Println("update; merges with later ones:", u.MergesWithLater())
	})
	defer cancel()

	src.Update(func(c *snapshot.Change[string, string]) {
		c.EditGroup("routes", func(e *snapshot.Editor) { e.Put(routeTo("new")) })
		c.Then()
		c.EditGroup("clusters", func(e *snapshot.Editor) { e.Remove(xds.ClusterType, "old") })
	})
	// Output:
	// update; merges with later ones: false
	// update; merges with later ones: true
}
