package xds_test

import (
	"slices"
	"testing"
	"testing/synctest"
	"time"

	discoveryv3 "github.com/envoyproxy/go-control-plane/envoy/service/discovery/v3"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	xds "github.com/JonathanO/go-envoy-xds"
	"github.com/JonathanO/go-envoy-xds/snapshot"
)

func TestDeltaTellsEnvoyWhatItAskedForDoesNotExist(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		e := startDelta(t, newServer(t, newSource(cla("a", "10.0.0.1")), xds.EnvoyConfig[string]{}, xds.Events[string]{}))

		eds := e.subscribe(xds.EndpointType, "a", "gone")
		if len(eds) != 1 || !slices.Equal(deltaNames(eds[0]), []string{"a"}) || !slices.Equal(eds[0].GetRemovedResources(), []string{"gone"}) {
			t.Fatalf("got %v, want a, and gone removed", eds)
		}
	})
}

func TestDeltaRoutesToANewClusterWaitForItsAck(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		src := newSource(cluster("a"), route("r", "a"))
		e := startDelta(t, newServer(t, src, xds.EnvoyConfig[string]{}, xds.Events[string]{}))
		e.ack(e.subscribe(xds.ClusterType)[0])
		e.ack(e.subscribe(xds.RouteType, "r")[0])

		update(src, cluster("b"), route("r", "b"))
		cds := e.sent()
		if len(cds) != 1 || !slices.Equal(deltaNames(cds[0]), []string{"b"}) {
			t.Fatalf("got %v, want just the new cluster first", cds)
		}
		if rds := e.ack(cds[0]); !slices.Equal(types(rds), []xds.TypeURL{xds.RouteType}) {
			t.Fatalf("got %v after the cluster ACK, want the route", rds)
		}
	})
}

func TestDeltaUnsubscribingIsNotAnswered(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		e := startDelta(t, newServer(t, newSource(cla("a", "10.0.0.1")), xds.EnvoyConfig[string]{}, xds.Events[string]{}))
		e.ack(e.subscribe(xds.EndpointType, "a")[0])

		if sent := e.send(&discoveryv3.DeltaDiscoveryRequest{Node: node, TypeUrl: string(xds.EndpointType),
			ResourceNamesUnsubscribe: []string{"a"}}); len(sent) != 0 {
			t.Fatalf("got %v, want no answer", sent)
		}
	})
}

func TestDeltaSendsNoneOfAChangeAgainThatASubscriptionWasAnsweredWith(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		src := newSource(cluster("a"), cla("a", "10.0.0.1"))
		e := startDelta(t, newServer(t, src, xds.EnvoyConfig[string]{}, xds.Events[string]{}))
		e.ack(e.subscribe(xds.ClusterType)[0])
		e.ack(e.subscribe(xds.EndpointType, "a")[0])

		// A change adds cluster b and its endpoints. Envoy asks for b's endpoints before the change gets to EDS.
		update(src, cluster("b"), cla("b", "10.0.0.2"))
		cds := e.sent()[0]
		eds := e.subscribe(xds.EndpointType, "b")
		if len(eds) != 1 || !slices.Equal(deltaNames(eds[0]), []string{"b"}) {
			t.Fatalf("got %v, want b's endpoints", eds)
		}
		e.ack(eds[0])
		if sent := e.ack(cds); len(sent) != 0 {
			t.Fatalf("got %v after the cluster ACK, want nothing: b's endpoints already went", sent)
		}

		// What a change skipped isn't skipped for good: b moves, then moves back.
		for _, address := range []string{"10.0.0.3", "10.0.0.2"} {
			update(src, cla("b", address))
			sent := e.sent()
			if len(sent) != 1 || !slices.Equal(deltaNames(sent[0]), []string{"b"}) {
				t.Fatalf("moving b to %s: got %v", address, sent)
			}
			e.ack(sent[0])
		}
	})
}

func TestDeltaRemovesWhatAReconnectingEnvoyKeptOnceTheRoutesAreAcked(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		src := newSource(cluster("a"), route("r", "a"))
		cfg := xds.EnvoyConfig[string]{SubscribesTo: []xds.TypeURL{xds.ClusterType, xds.RouteType}}
		e := startDelta(t, newServer(t, src, cfg, xds.Events[string]{}))

		// Envoy kept cluster gone, which its kept routes may still use.
		cds := e.send(&discoveryv3.DeltaDiscoveryRequest{Node: node, TypeUrl: string(xds.ClusterType),
			InitialResourceVersions: map[string]string{"a": "1", "gone": "1"}})
		if len(cds) != 1 || len(cds[0].GetRemovedResources()) != 0 {
			t.Fatalf("got %v, want gone kept for now", cds)
		}
		e.ack(cds[0])
		rds := e.subscribe(xds.RouteType, "r")
		if sent := e.ack(rds[0]); len(sent) != 1 || !slices.Equal(sent[0].GetRemovedResources(), []string{"gone"}) {
			t.Fatalf("got %v after the route ACK, want gone removed", sent)
		}
	})
}

func TestDeltaGivesUpWaitingForTypesTheEnvoyNeverAsksFor(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		src := newSource(cluster("a"))
		e := startDelta(t, newServer(t, src, xds.EnvoyConfig[string]{}, xds.Events[string]{}))
		e.ack(e.send(&discoveryv3.DeltaDiscoveryRequest{Node: node, TypeUrl: string(xds.ClusterType),
			InitialResourceVersions: map[string]string{"gone": "1"}})[0])

		time.Sleep(30 * time.Second)
		update(src, cluster("b")) // the removal goes at the next event, and the change after its ACK
		removal := e.sent()
		if len(removal) != 1 || !slices.Equal(removal[0].GetRemovedResources(), []string{"gone"}) {
			t.Fatalf("got %v, want gone removed", removal)
		}
		if sent := e.ack(removal[0]); len(sent) != 1 || !slices.Equal(deltaNames(sent[0]), []string{"b"}) {
			t.Fatalf("got %v after the removal's ACK, want b", sent)
		}
	})
}

func TestDeltaSubscribingToAndUnsubscribingFromANameAtOnceEndsTheStream(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		e := startDelta(t, newServer(t, newSource(), xds.EnvoyConfig[string]{}, xds.Events[string]{}))
		e.stream.reqs <- &discoveryv3.DeltaDiscoveryRequest{Node: node, TypeUrl: string(xds.EndpointType),
			ResourceNamesSubscribe: []string{"a"}, ResourceNamesUnsubscribe: []string{"a"}}
		if err := <-e.ended; status.Code(err) != codes.InvalidArgument {
			t.Fatalf("got %v, want InvalidArgument", err)
		}
	})
}

func TestDeltaLeavesUnknownTypesUnanswered(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		e := startDelta(t, newServer(t, newSource(cluster("a")), xds.EnvoyConfig[string]{}, xds.Events[string]{}))
		if sent := e.subscribe("type.googleapis.com/envoy.config.core.v3.TypedExtensionConfig", "x"); len(sent) != 0 {
			t.Fatalf("got %v", sent)
		}
		if cds := e.subscribe(xds.ClusterType); len(cds) != 1 {
			t.Fatalf("got %v, want the stream still up", cds)
		}
	})
}

func TestDelayUpdatesUntilAckOfHoldsUpdatesUntilThatAck(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		src := newSource(cluster("a"), route("r", "a"))
		e := startDelta(t, newServer(t, src, xds.EnvoyConfig[string]{DelayUpdatesUntilAckOf: xds.RouteType}, xds.Events[string]{}))
		e.ack(e.subscribe(xds.ClusterType)[0])

		update(src, cluster("b"))
		if sent := e.sent(); len(sent) != 0 {
			t.Fatalf("got %v before RDS is ACKed", sent)
		}
		if sent := e.ack(e.subscribe(xds.RouteType, "r")[0]); len(sent) != 1 || !slices.Equal(deltaNames(sent[0]), []string{"b"}) {
			t.Fatalf("got %v after the RDS ACK, want b", sent)
		}
	})
}

func TestRoutesWaitForTheClustersAnEnvoyIsBeingSent(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		src := newSource(cluster("a"), route("r", "a"))
		e := startDelta(t, newServer(t, src, xds.EnvoyConfig[string]{}, xds.Events[string]{}))
		cds := e.subscribe(xds.ClusterType)[0]

		// A reconnecting Envoy asks for everything at once.
		if rds := e.subscribe(xds.RouteType, "r"); len(rds) != 0 {
			t.Fatalf("got %v while CDS isn't ACKed", rds)
		}
		if rds := e.ack(cds); !slices.Equal(types(rds), []xds.TypeURL{xds.RouteType}) {
			t.Fatalf("got %v after the CDS ACK, want the routes", rds)
		}
	})
}

func TestEachPhaseIsAckedBeforeTheNext(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		src := newSource(cluster("a"), route("r", "a"))
		e := startDelta(t, newServer(t, src, xds.EnvoyConfig[string]{}, xds.Events[string]{}))
		e.ack(e.subscribe(xds.ClusterType)[0])
		e.ack(e.subscribe(xds.RouteType, "r")[0])

		// Make before break alone would send the cluster first.
		src.Update(func(c *snapshot.Change[string, string]) {
			c.EditGroup("g", func(e *snapshot.Editor) { e.Put(route("r", "b")) })
			c.Then()
			c.EditGroup("g", func(e *snapshot.Editor) { e.Put(cluster("a2")) })
		})
		rds := e.sent()
		if !slices.Equal(types(rds), []xds.TypeURL{xds.RouteType}) {
			t.Fatalf("got %v, want the first phase's route alone", rds)
		}
		if cds := e.ack(rds[0]); !slices.Equal(types(cds), []xds.TypeURL{xds.ClusterType}) {
			t.Fatalf("got %v after the route ACK, want the second phase", cds)
		}
	})
}

func TestDelayUpdatesUntilAckOfGivesUpAfterThirtySeconds(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		src := newSource(cluster("a"))
		e := startDelta(t, newServer(t, src, xds.EnvoyConfig[string]{DelayUpdatesUntilAckOf: xds.RouteType}, xds.Events[string]{}))
		e.ack(e.subscribe(xds.ClusterType)[0])

		time.Sleep(30 * time.Second)
		update(src, cluster("b"))
		if sent := e.sent(); len(sent) != 1 {
			t.Fatalf("got %v, want b sent anyway", sent)
		}
	})
}

func TestDelayUpdatesUntilAckOfATypeTheEnvoyDoesNotGetIsIgnored(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		src := newSource(cluster("a"))
		cfg := xds.EnvoyConfig[string]{DelayUpdatesUntilAckOf: xds.RouteType, SubscribesTo: []xds.TypeURL{xds.ClusterType}}
		e := startDelta(t, newServer(t, src, cfg, xds.Events[string]{}))
		e.ack(e.subscribe(xds.ClusterType)[0])

		update(src, cluster("b"))
		if sent := e.sent(); len(sent) != 1 {
			t.Fatalf("got %v, want b", sent)
		}
	})
}

func TestAResourceThatCannotBeSentEndsTheStream(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		e := startDelta(t, newServer(t, newSource(cluster("bad\xff")), xds.EnvoyConfig[string]{}, xds.Events[string]{}))
		e.stream.reqs <- &discoveryv3.DeltaDiscoveryRequest{Node: node, TypeUrl: string(xds.ClusterType)}
		if err := <-e.ended; err == nil {
			t.Fatal("the stream ended OK")
		}
	})
}
