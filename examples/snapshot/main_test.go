package main

import (
	"net/netip"
	"slices"
	"testing"

	routev3 "github.com/envoyproxy/go-control-plane/envoy/config/route/v3"

	xds "github.com/JonathanO/go-envoy-xds"
)

func TestCommandsKeepTheClustersAndEachPortsRoutesInStep(t *testing.T) {
	m := newMesh()

	m.addPort(8080)
	m.add("a", netip.MustParseAddrPort("10.0.0.1:80"))

	check(t, m.src.View(8080), "routes-8080", "clusters")
	check(t, names(m.src.Group("clusters"), xds.ClusterType), "a")
	if got := len(m.endpoints("a")); got != 1 {
		t.Errorf("got %d endpoints, want 1", got)
	}
	hosts := routes(t, m).GetVirtualHosts()
	if len(hosts) != 1 {
		t.Fatalf("got %d virtual hosts, want 1", len(hosts))
	}
	check(t, hosts[0].GetDomains(), "a", "a:8080")

	m.remove("a")

	check(t, names(m.src.Group("clusters"), xds.ClusterType))
	check(t, names(m.src.Group("clusters"), xds.EndpointType))
	if got := routes(t, m).GetVirtualHosts(); len(got) > 0 {
		t.Errorf("got virtual hosts %v, want none", got)
	}
}

func routes(t *testing.T, m *mesh) *routev3.RouteConfiguration {
	t.Helper()
	rc, ok := m.src.Group("routes-8080").Resource(xds.RouteType, "all_routes")
	if !ok {
		t.Fatal("no route config")
	}
	return rc.(*routev3.RouteConfiguration)
}

func check(t *testing.T, got []string, want ...string) {
	t.Helper()
	if !slices.Equal(got, want) {
		t.Errorf("got %q, want %q", got, want)
	}
}
