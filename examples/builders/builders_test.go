package main

import (
	"context"
	"fmt"
	"net"
	"net/netip"
	"slices"
	"testing"
	"time"

	clusterv3 "github.com/envoyproxy/go-control-plane/envoy/config/cluster/v3"
	corev3 "github.com/envoyproxy/go-control-plane/envoy/config/core/v3"
	endpointv3 "github.com/envoyproxy/go-control-plane/envoy/config/endpoint/v3"
	routev3 "github.com/envoyproxy/go-control-plane/envoy/config/route/v3"
	discoveryv3 "github.com/envoyproxy/go-control-plane/envoy/service/discovery/v3"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/proto"

	xds "github.com/JonathanO/go-envoy-xds"
)

// The builders, driven by the cluster manager: each change goes in the right round, adds and changes in add order
// and removes in remove order, and an ADS stream sends them make before break.

const port = 8080

func all(string) bool { return true }

func is(name string) func(string) bool { return func(n string) bool { return n == name } }

var (
	clusters  clusterBuilder
	endpoints endpointBuilder
	routes    routeBuilder
)

func TestAddingAServiceAddsItsClusterEndpointsAndRouteInAddOrder(t *testing.T) {
	m := newClusterManager()
	m.addEndpoint("a", ip("10.0.0.1"))

	u := change(m, func() { m.addEndpoint("b", ip("10.0.0.2")) })

	check(t, names(clusters.AddOrder(u, all, port).Updates), "b")
	added := endpoints.AddOrder(u, all, port).Updates
	check(t, names(added), "b")
	check(t, addresses(added[0]), "10.0.0.2:80")
	check(t, virtualHosts(t, routes.AddOrder(u, all, port)), "a", "b")

	noop(t, clusters.RemoveOrder(u, all, port))
	noop(t, endpoints.RemoveOrder(u, all, port))
	noop(t, routes.RemoveOrder(u, all, port))
}

func TestReplacingAnEndpointAddsTheNewOneBeforeRemovingTheOld(t *testing.T) {
	m := newClusterManager()
	m.addEndpoint("a", ip("10.0.0.1"))

	u := change(m, func() { m.setEndpoints("a", ip("10.0.0.2")) })

	added := endpoints.AddOrder(u, all, port).Updates
	check(t, names(added), "a")
	check(t, addresses(added[0]), "10.0.0.1:80", "10.0.0.2:80")
	removed := endpoints.RemoveOrder(u, all, port)
	check(t, names(removed.Updates), "a")
	check(t, addresses(removed.Updates[0]), "10.0.0.2:80")
	check(t, removed.Removes)

	noop(t, clusters.AddOrder(u, all, port))
	noop(t, clusters.RemoveOrder(u, all, port))
	noop(t, routes.AddOrder(u, all, port))
	noop(t, routes.RemoveOrder(u, all, port))
}

func TestRemovingAServiceRemovesItsRouteClusterAndEndpointsInRemoveOrder(t *testing.T) {
	m := newClusterManager()
	m.addEndpoint("a", ip("10.0.0.1"))
	m.addEndpoint("b", ip("10.0.0.2"))

	u := change(m, func() { m.removeService("a") })

	noop(t, clusters.AddOrder(u, all, port))
	noop(t, endpoints.AddOrder(u, all, port))
	noop(t, routes.AddOrder(u, all, port))

	// The Server sends the route before the cluster in remove order, so nothing is left pointing at a.
	check(t, virtualHosts(t, routes.RemoveOrder(u, all, port)), "b")
	check(t, clusters.RemoveOrder(u, all, port).Removes, "a")
	removed := endpoints.RemoveOrder(u, all, port)
	check(t, names(removed.Updates))
	check(t, removed.Removes, "a")
}

func TestSubscribingMidChangeGetsTheStateOfThatPhase(t *testing.T) {
	m := newClusterManager()
	m.addEndpoint("a", ip("10.0.0.1"))

	u := change(m, func() {
		m.removeService("a")
		m.addEndpoint("b", ip("10.0.0.2"))
	})

	// Until remove order, a is still there; b is there from add order on.
	check(t, names(clusters.ResourcesAddOrder(u, all, port)), "a", "b")
	check(t, names(clusters.ResourcesRemoveOrder(u, all, port)), "b")
	check(t, names(endpoints.ResourcesAddOrder(u, is("a"), port)), "a")
	check(t, names(endpoints.ResourcesRemoveOrder(u, is("a"), port)))
	check(t, virtualHosts(t, xds.Response{Updates: routes.ResourcesAddOrder(u, all, port)}), "a", "b")
	check(t, virtualHosts(t, xds.Response{Updates: routes.ResourcesRemoveOrder(u, all, port)}), "b")
	check(t, names(routes.ResourcesAddOrder(u, is("other"), port)))
}

func TestSubscribingMidChangeToChangingEndpointsGetsWhatThatPhaseSends(t *testing.T) {
	m := newClusterManager()
	m.addEndpoint("a", ip("10.0.0.1"))

	u := change(m, func() { m.setEndpoints("a", ip("10.0.0.2")) })

	// E.g. Envoy asking again for a's endpoints after the change's CDS: the old endpoint mustn't go before remove order.
	during := endpoints.ResourcesAddOrder(u, all, port)
	check(t, names(during), "a")
	check(t, addresses(during[0]), "10.0.0.1:80", "10.0.0.2:80")
	after := endpoints.ResourcesRemoveOrder(u, all, port)
	check(t, names(after), "a")
	check(t, addresses(after[0]), "10.0.0.2:80")
}

func TestSotwStreamGetsChangesMakeBeforeBreak(t *testing.T) {
	m := newClusterManager()
	m.addEndpoint("a", ip("10.0.0.1"))
	m.addEndpoint("b", ip("10.0.0.2"))
	envoy := connect(t, m)

	envoy.request(xds.ClusterType)
	envoy.expect("CDS [a b]")
	envoy.request(xds.EndpointType, "a", "b")
	envoy.expect("EDS [a b]")
	envoy.request(xds.RouteType, "all_routes")
	envoy.expect("RDS [a b]")

	// Make: the cluster goes before the route that sends traffic to it.
	m.addEndpoint("c", ip("10.0.0.3"))
	envoy.expect("CDS [a b c]", "RDS [a b c]")

	// Break: the route stops sending traffic to the cluster before it goes.
	m.removeService("a")
	envoy.expect("RDS [b c]", "CDS [b c]", "EDS [b]")
}

// envoy is Envoy's end of a SotW stream: it asks for names of a type, and ACKs what it's sent.
type envoy struct {
	t             *testing.T
	stream        grpc.BidiStreamingClient[discoveryv3.DiscoveryRequest, discoveryv3.DiscoveryResponse]
	subscriptions map[string]*discoveryv3.DiscoveryRequest // by type
}

// connect serves m over gRPC, and opens a stream to it.
func connect(t *testing.T, m *clusterManager) *envoy {
	srv, err := newServer(m)
	if err != nil {
		t.Fatal(err)
	}
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	gs := grpc.NewServer()
	discoveryv3.RegisterAggregatedDiscoveryServiceServer(gs, srv)
	go gs.Serve(lis)
	t.Cleanup(gs.Stop)
	conn, err := grpc.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	t.Cleanup(cancel)
	stream, err := discoveryv3.NewAggregatedDiscoveryServiceClient(conn).StreamAggregatedResources(ctx)
	if err != nil {
		t.Fatal(err)
	}
	return &envoy{t: t, stream: stream, subscriptions: map[string]*discoveryv3.DiscoveryRequest{}}
}

func (e *envoy) request(typ xds.TypeURL, names ...string) {
	req := &discoveryv3.DiscoveryRequest{Node: &corev3.Node{Id: "envoy"}, TypeUrl: string(typ), ResourceNames: names}
	e.subscriptions[req.TypeUrl] = req
	if err := e.stream.Send(req); err != nil {
		e.t.Fatal(err)
	}
}

// expect waits for each of the next responses, ACKs it with the names subscribed to, and fails unless it's as want
// describes it, in order.
func (e *envoy) expect(want ...string) {
	e.t.Helper()
	for _, w := range want {
		resp, err := e.stream.Recv()
		if err != nil {
			e.t.Fatal(err)
		}
		ack := proto.Clone(e.subscriptions[resp.TypeUrl]).(*discoveryv3.DiscoveryRequest)
		ack.VersionInfo, ack.ResponseNonce = resp.VersionInfo, resp.Nonce
		if err := e.stream.Send(ack); err != nil {
			e.t.Fatal(err)
		}
		if got := describe(e.t, resp); got != w {
			e.t.Errorf("got %s, want %s", got, w)
		}
	}
}

var short = map[string]string{string(xds.ClusterType): "CDS", string(xds.EndpointType): "EDS", string(xds.RouteType): "RDS"}

// describe gives a response's type, and the clusters it names.
func describe(t *testing.T, resp *discoveryv3.DiscoveryResponse) string {
	t.Helper()
	var names []string
	for _, a := range resp.Resources {
		m, err := a.UnmarshalNew()
		if err != nil {
			t.Fatal(err)
		}
		switch m := m.(type) {
		case *clusterv3.Cluster:
			names = append(names, m.Name)
		case *endpointv3.ClusterLoadAssignment:
			names = append(names, m.ClusterName)
		case *routev3.RouteConfiguration:
			names = append(names, hosts(m)...)
		}
	}
	slices.Sort(names)
	return fmt.Sprintf("%s %v", short[resp.TypeUrl], names)
}

// change is the update the cluster manager sends for a change.
func change(m *clusterManager, f func()) update {
	before := m.state()
	f()
	return update{before, m.state()}
}

func ip(host string) netip.AddrPort { return netip.AddrPortFrom(netip.MustParseAddr(host), 80) }

func check(t *testing.T, got []string, want ...string) {
	t.Helper()
	if !slices.Equal(got, want) {
		t.Errorf("got %q, want %q", got, want)
	}
}

func noop(t *testing.T, r xds.Response) {
	t.Helper()
	if len(r.Updates) > 0 || len(r.Removes) > 0 {
		t.Errorf("got %v, want no change", r)
	}
}

// names, addresses and hosts are sorted, as order doesn't matter to Envoy.

func names(rs []xds.Resource) []string {
	var ns []string
	for _, r := range rs {
		ns = append(ns, r.Name)
	}
	slices.Sort(ns)
	return ns
}

func addresses(cla xds.Resource) []string {
	var as []string
	for _, e := range cla.Message.(*endpointv3.ClusterLoadAssignment).Endpoints[0].LbEndpoints {
		a := e.GetEndpoint().GetAddress().GetSocketAddress()
		as = append(as, fmt.Sprintf("%s:%d", a.GetAddress(), a.GetPortValue()))
	}
	slices.Sort(as)
	return as
}

// virtualHosts are the virtual hosts of the one route config r updates.
func virtualHosts(t *testing.T, r xds.Response) []string {
	t.Helper()
	if len(r.Updates) != 1 || len(r.Removes) > 0 {
		t.Fatalf("got %v, want one route config", r)
	}
	return hosts(r.Updates[0].Message.(*routev3.RouteConfiguration))
}

func hosts(rc *routev3.RouteConfiguration) []string {
	var hs []string
	for _, h := range rc.VirtualHosts {
		hs = append(hs, h.Name)
	}
	slices.Sort(hs)
	return hs
}
