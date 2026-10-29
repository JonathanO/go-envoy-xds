package xds_test

import (
	"slices"
	"testing"
	"testing/synctest"

	routev3 "github.com/envoyproxy/go-control-plane/envoy/config/route/v3"
	tlsv3 "github.com/envoyproxy/go-control-plane/envoy/extensions/transport_sockets/tls/v3"
	discoveryv3 "github.com/envoyproxy/go-control-plane/envoy/service/discovery/v3"
	runtimev3 "github.com/envoyproxy/go-control-plane/envoy/service/runtime/v3"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	xds "github.com/JonathanO/go-envoy-xds"
	"github.com/JonathanO/go-envoy-xds/snapshot"
)

func TestAChangeGoesOutInAddOrderThenRemoveOrder(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		every := []xds.TypeURL{xds.SecretType, xds.RuntimeType, xds.ClusterType, xds.EndpointType, xds.ListenerType,
			xds.RouteType, xds.ScopedRouteType, xds.VirtualHostType}
		src := newSource()
		e := startDelta(t, newServer(t, src, xds.EnvoyConfig[string]{}, xds.Events[string]{}, serving(every...)))
		for _, typ := range every {
			e.ack(e.subscribe(typ, "x")[0])
		}
		// pushed ACKs a change's responses, one at a time, and returns their types.
		pushed := func() []xds.TypeURL {
			var ts []xds.TypeURL
			for sent := e.sent(); len(sent) > 0; sent = e.ack(sent[0]) {
				if len(sent) != 1 {
					t.Fatalf("got %v at once, want one type at a time", types(sent))
				}
				ts = append(ts, xds.TypeURL(sent[0].GetTypeUrl()))
			}
			return ts
		}

		update(src, &tlsv3.Secret{Name: "x"}, &runtimev3.Runtime{Name: "x"}, cluster("x"), cla("x", "10.0.0.1"), listener("x"),
			route("x", "x"), &routev3.ScopedRouteConfiguration{Name: "x"}, &routev3.VirtualHost{Name: "x"})
		if got := pushed(); !slices.Equal(got, every) {
			t.Errorf("added in order %v", got)
		}
		edit(src, func(e *snapshot.Editor) {
			for _, typ := range every {
				e.Remove(typ, "x")
			}
		})
		want := []xds.TypeURL{xds.ListenerType, xds.ScopedRouteType, xds.RouteType, xds.VirtualHostType, xds.ClusterType,
			xds.EndpointType, xds.RuntimeType, xds.SecretType}
		if got := pushed(); !slices.Equal(got, want) {
			t.Errorf("removed in order %v", got)
		}
	})
}

func TestVersionsCountUpFromOnePerType(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		src := newSource(cluster("a"), cla("a", "10.0.0.1"))
		e := startDelta(t, newServer(t, src, xds.EnvoyConfig[string]{}, xds.Events[string]{}))
		var versions []string
		for _, r := range []*discoveryv3.DeltaDiscoveryResponse{e.subscribe(xds.ClusterType)[0], e.subscribe(xds.EndpointType, "a")[0]} {
			versions = append(versions, r.GetSystemVersionInfo())
			e.ack(r)
		}
		update(src, cluster("b"))
		versions = append(versions, e.sent()[0].GetSystemVersionInfo())
		if want := []string{"1", "1", "2"}; !slices.Equal(versions, want) {
			t.Fatalf("versions %v, want %v", versions, want)
		}
	})
}

func TestRoutesWaitForTheClustersAndEndpointsAnEnvoyIsBeingSent(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		src := newSource(cluster("a"), cla("a", "10.0.0.1"), route("r", "a"))
		e := startDelta(t, newServer(t, src, xds.EnvoyConfig[string]{}, xds.Events[string]{}))
		cds := e.subscribe(xds.ClusterType)[0]

		// A reconnecting Envoy asks for everything at once.
		if rds := e.subscribe(xds.RouteType, "r"); len(rds) != 0 {
			t.Fatalf("got %v while CDS isn't ACKed", rds)
		}
		eds := e.subscribe(xds.EndpointType, "a")
		if !slices.Equal(types(eds), []xds.TypeURL{xds.EndpointType}) {
			t.Fatalf("got %v, want the endpoints alone", eds)
		}
		if sent := e.ack(cds); len(sent) != 0 {
			t.Fatalf("got %v while EDS isn't ACKed", sent)
		}
		if rds := e.ack(eds[0]); !slices.Equal(types(rds), []xds.TypeURL{xds.RouteType}) {
			t.Fatalf("got %v after the EDS ACK, want the routes", rds)
		}
	})
}

func TestAnAckOfRoutesIsNeverHeld(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		src := newSource(cluster("a"), cla("a", "10.0.0.1"), listener("l"), route("r", "a"))
		e := startDelta(t, newServer(t, src, xds.EnvoyConfig[string]{}, xds.Events[string]{}))
		e.ack(e.subscribe(xds.ClusterType)[0])
		e.ack(e.subscribe(xds.ListenerType)[0])
		e.ack(e.subscribe(xds.RouteType, "r")[0])

		edit(src, func(e *snapshot.Editor) {
			e.Put(route("r", "b"))
			e.Remove(xds.ListenerType, "l")
		})
		rds := e.sent()[0]
		e.subscribe(xds.EndpointType, "a") // so the clusters await an ACK
		if lds := e.ack(rds); !slices.Equal(types(lds), []xds.TypeURL{xds.ListenerType}) || !slices.Equal(lds[0].GetRemovedResources(), []string{"l"}) {
			t.Fatalf("got %v after the route ACK, want the listener removed", lds)
		}
	})
}

func TestAHeldRequestThatIsInvalidEndsTheStream(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		e := startDelta(t, newServer(t, newSource(cluster("a")), xds.EnvoyConfig[string]{}, xds.Events[string]{}))
		cds := e.subscribe(xds.ClusterType)[0]
		e.send(&discoveryv3.DeltaDiscoveryRequest{Node: node, TypeUrl: string(xds.RouteType),
			ResourceNamesSubscribe: []string{"r"}, ResourceNamesUnsubscribe: []string{"r"}})
		e.stream.reqs <- &discoveryv3.DeltaDiscoveryRequest{Node: node, TypeUrl: string(xds.ClusterType), ResponseNonce: cds.GetNonce()}
		if err := <-e.ended; status.Code(err) != codes.InvalidArgument {
			t.Fatalf("got %v, want InvalidArgument", err)
		}
	})
}

func TestStaleListenersAreRemovedStraightAway(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		e := startDelta(t, newServer(t, newSource(listener("l")), xds.EnvoyConfig[string]{}, xds.Events[string]{}))
		// Nothing refers to listeners, so nothing need be ACKed first.
		lds := e.reconnect(xds.ListenerType, []string{"l", "gone"}, "*")
		if len(lds) != 1 || !slices.Equal(deltaNames(lds[0]), []string{"l"}) || !slices.Equal(lds[0].GetRemovedResources(), []string{"gone"}) {
			t.Fatalf("got %v, want l, and gone removed", lds)
		}
	})
}

func TestStaleRemovalsWaitOnlyForTypesThatAreServed(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		src := newSource(cluster("a"), listener("l"), route("r", "a"))
		e := startDelta(t, newServer(t, src, xds.EnvoyConfig[string]{}, xds.Events[string]{}))
		e.ack(e.reconnect(xds.ClusterType, []string{"a", "gone"})[0])
		e.ack(e.subscribe(xds.ListenerType)[0])
		// Nothing serves scoped routes or virtual hosts.
		if sent := e.ack(e.subscribe(xds.RouteType, "r")[0]); len(sent) != 1 || !slices.Equal(sent[0].GetRemovedResources(), []string{"gone"}) {
			t.Fatalf("got %v after the route ACK, want gone removed", sent)
		}
	})
}

func TestStaleRemovalsWaitForTheAckOfAnEarlierType(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		cfg := xds.EnvoyConfig[string]{SubscribesTo: []xds.TypeURL{xds.ListenerType, xds.ClusterType}}
		e := startDelta(t, newServer(t, newSource(cluster("a")), cfg, xds.Events[string]{}))
		lds := e.subscribe(xds.ListenerType)[0]
		cds := e.reconnect(xds.ClusterType, []string{"gone"})
		if len(cds) != 1 || len(cds[0].GetRemovedResources()) != 0 {
			t.Fatalf("got %v, want gone kept until LDS is ACKed", cds)
		}
		e.ack(cds[0])
		if sent := e.ack(lds); len(sent) != 1 || !slices.Equal(sent[0].GetRemovedResources(), []string{"gone"}) {
			t.Fatalf("got %v after the LDS ACK, want gone removed", sent)
		}
	})
}

func TestStaleRemovalsSkipWhatsBackSince(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		src := newSource(cluster("a"), route("r", "a"))
		cfg := xds.EnvoyConfig[string]{SubscribesTo: []xds.TypeURL{xds.ClusterType, xds.RouteType}}
		e := startDelta(t, newServer(t, src, cfg, xds.Events[string]{}))
		e.ack(e.reconnect(xds.ClusterType, []string{"a", "old", "older"})[0])
		update(src, cluster("old"), cluster("older"))
		e.ack(e.sent()[0])
		if sent := e.ack(e.subscribe(xds.RouteType, "r")[0]); len(sent) != 0 {
			t.Fatalf("got %v after the route ACK, want nothing: old and older are back", sent)
		}
	})
}

func TestNackStopsTheRestOfTheChange(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		src := newSource(cluster("a"), route("r", "a"))
		w := &watched{}
		e := startDelta(t, newServer(t, src, xds.EnvoyConfig[string]{}, xds.Events[string]{}, w.option))
		e.ack(e.subscribe(xds.ClusterType)[0])
		e.ack(e.subscribe(xds.RouteType, "r")[0])

		update(src, cluster("b"), route("r", "b"))
		if sent := e.nack(e.sent()[0]); len(sent) != 0 {
			t.Fatalf("got %v after the NACK, want no more of the change", sent)
		}
		if w.cancelled != 1 {
			t.Fatalf("still subscribed to the source after a NACK")
		}
		e.end()
		synctest.Wait()
		if w.cancelled != 1 {
			t.Fatalf("unsubscribed %d times", w.cancelled)
		}
	})
}

func TestNackStopsStaleRemovals(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		src := newSource(cluster("a"), route("r", "a"))
		cfg := xds.EnvoyConfig[string]{SubscribesTo: []xds.TypeURL{xds.ClusterType, xds.RouteType}}
		l := &logs{}
		e := startDelta(t, newServer(t, src, cfg, xds.Events[string]{}, l.option))
		e.ack(e.reconnect(xds.ClusterType, []string{"a", "gone"})[0])
		if sent := e.nack(e.subscribe(xds.RouteType, "r")[0]); len(sent) != 0 {
			t.Fatalf("got %v after the NACK, want gone left alone", sent)
		}
		if got := l.count("Envoy rejected config"); got != 1 {
			t.Fatalf("warned of %d NACKs", got)
		}
	})
}

func TestMetricsFollowPushesAndAcks(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		src := newSource(cluster("a"), cla("a", "10.0.0.1"), cla("b", "10.0.0.2"), route("r", "a"))
		cfg := xds.EnvoyConfig[string]{SubscribesTo: []xds.TypeURL{xds.ClusterType, xds.EndpointType, xds.RouteType}}
		m := &metrics{}
		e := startDelta(t, newServer(t, src, cfg, xds.Events[string]{}, m.option))
		e.ack(e.reconnect(xds.ClusterType, []string{"gone"})[0])
		e.ack(e.ack(e.subscribe(xds.RouteType, "r")[0])[0]) // then gone's removal
		first := e.subscribe(xds.EndpointType, "a")[0]
		second := e.subscribe(xds.EndpointType, "b")[0] // still waiting for an ACK of EDS
		e.ack(first)                                    // not the latest
		e.ack(second)
		update(src, cluster("b"))
		e.ack(e.sent()[0])
		update(src, cluster("c"), route("r", "c"))
		e.nack(e.sent()[0])
		e.end()
		synctest.Wait()

		want := []string{"awaiting", "acked", "awaiting", "acked", "awaiting", "acked", "awaiting", "acked",
			"began", "completed", "began", "completed", "closed"}
		if !slices.Equal(m.calls, want) {
			t.Fatalf("got %v,\nwant %v", m.calls, want)
		}
	})
}
