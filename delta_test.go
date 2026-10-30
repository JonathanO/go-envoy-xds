package xds_test

import (
	"fmt"
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

		// A change adds cluster b and its endpoints, and moves a's. Envoy asks for b's endpoints before the change gets
		// to EDS.
		update(src, cluster("b"), cla("a", "10.0.0.3"), cla("b", "10.0.0.2"))
		cds := e.sent()[0]
		eds := e.subscribe(xds.EndpointType, "b")
		if len(eds) != 1 || !slices.Equal(deltaNames(eds[0]), []string{"b"}) {
			t.Fatalf("got %v, want b's endpoints", eds)
		}
		e.ack(eds[0])
		rest := e.ack(cds)
		if len(rest) != 1 || !slices.Equal(deltaNames(rest[0]), []string{"a"}) {
			t.Fatalf("got %v after the cluster ACK, want a's endpoints alone: b's already went", rest)
		}
		e.ack(rest[0])

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

func TestDeltaSubscriptionBeforeItsTypesRemoveRoundGetsWhatTheRoundRemovesThenItsRemoval(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		src := newSource(cluster("a"), cla("a", "10.0.0.1"), cla("x", "10.0.0.9"), route("r", "a"))
		e := startDelta(t, newServer(t, src, xds.EnvoyConfig[string]{}, xds.Events[string]{}))
		e.ack(e.subscribe(xds.ClusterType)[0])
		e.ack(e.subscribe(xds.RouteType, "r")[0])
		e.ack(e.subscribe(xds.EndpointType, "a")[0])

		edit(src, func(e *snapshot.Editor) {
			e.Remove(xds.RouteType, "r")
			e.Remove(xds.EndpointType, "x")
		})
		rds := e.sent()[0]
		eds := e.subscribe(xds.EndpointType, "x")
		if len(eds) != 1 || !slices.Equal(deltaNames(eds[0]), []string{"x"}) || len(eds[0].GetRemovedResources()) != 0 {
			t.Fatalf("got %v, want x: its removal waits for the routes'", eds)
		}
		e.ack(eds[0])
		if sent := e.ack(rds); len(sent) != 1 || !slices.Equal(sent[0].GetRemovedResources(), []string{"x"}) {
			t.Fatalf("got %v after the route ACK, want x removed in turn", sent)
		}
	})
}

func TestDeltaReconnectingMidChangeKeepsWhatTheChangeRemovesUntilItsTurn(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		src := newSource(cluster("a"), cluster("x"), cla("a", "10.0.0.1"), listener("l"), route("r", "a"), route("r2", "x"))
		e := startDelta(t, newServer(t, src, xds.EnvoyConfig[string]{}, xds.Events[string]{}))
		e.ack(e.subscribe(xds.ListenerType)[0])
		e.ack(e.subscribe(xds.RouteType, "r", "r2")[0])
		e.ack(e.subscribe(xds.EndpointType, "a")[0])

		edit(src, func(e *snapshot.Editor) {
			e.Put(cla("a", "10.0.0.2"))
			e.Remove(xds.RouteType, "r2")
			e.Remove(xds.ClusterType, "x")
		})
		eds := e.sent()[0]
		// The reconnected Envoy asks for clusters last. It kept x, which r2 still routes to.
		cds := e.reconnect(xds.ClusterType, []string{"a", "x"}, "*")
		if len(cds) != 1 || !slices.Equal(deltaNames(cds[0]), []string{"a", "x"}) || len(cds[0].GetRemovedResources()) != 0 {
			t.Fatalf("got %v, want a and x: x isn't stale until its remove round", cds)
		}
		e.ack(cds[0])
		rds := e.ack(eds)
		if !slices.Equal(types(rds), []xds.TypeURL{xds.RouteType}) || !slices.Equal(rds[0].GetRemovedResources(), []string{"r2"}) {
			t.Fatalf("got %v after the endpoints' ACK, want r2 removed", rds)
		}
		if sent := e.ack(rds[0]); len(sent) != 1 || !slices.Equal(sent[0].GetRemovedResources(), []string{"x"}) {
			t.Fatalf("got %v after r2's removal is ACKed, want x removed", sent)
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

func TestDeltaIgnoresAcksOfEarlierResponses(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		src := newSource(cluster("a"))
		e := startDelta(t, newServer(t, src, xds.EnvoyConfig[string]{}, xds.Events[string]{}))
		first := e.subscribe(xds.ClusterType)[0]
		e.ack(first)
		update(src, cluster("b"))
		second := e.sent()[0]

		e.ack(first) // late
		update(src, cluster("c"))
		if sent := e.sent(); len(sent) != 0 {
			t.Fatalf("got %v, want nothing until Envoy answers %s", sent, second.GetNonce())
		}
		if sent := e.ack(second); len(sent) != 1 {
			t.Fatalf("got %v after the ACK, want c", sent)
		}
	})
}

func TestDeltaRemovesWhatAReconnectingEnvoyKeptOnceTheRoutesAreAcked(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		src := newSource(cluster("a"), cla("a", "10.0.0.1"), route("r", "a"))
		cfg := xds.EnvoyConfig[string]{SubscribesTo: []xds.TypeURL{xds.ClusterType, xds.EndpointType, xds.RouteType}}
		l := &logs{}
		e := startDelta(t, newServer(t, src, cfg, xds.Events[string]{}, l.option))

		// Envoy kept cluster gone, which its kept routes may still use.
		cds := e.reconnect(xds.ClusterType, []string{"a", "gone"})
		if len(cds) != 1 || len(cds[0].GetRemovedResources()) != 0 {
			t.Fatalf("got %v, want gone kept for now", cds)
		}
		e.ack(cds[0])
		rds := e.subscribe(xds.RouteType, "r")[0]
		eds := e.subscribe(xds.EndpointType, "a")[0]
		if sent := e.ack(rds); len(sent) != 0 {
			t.Fatalf("got %v, want nothing while Envoy has an answer to ACK", sent)
		}
		if sent := e.ack(eds); len(sent) != 1 || !slices.Equal(sent[0].GetRemovedResources(), []string{"gone"}) {
			t.Fatalf("got %v after the last ACK, want gone removed", sent)
		}
		if l.Len() != 0 {
			t.Fatalf("logged %s", l.String())
		}
	})
}

func TestDeltaGivesUpWaitingForTypesTheEnvoyNeverAsksFor(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		src := newSource(cluster("a"))
		l := &logs{}
		e := startDelta(t, newServer(t, src, xds.EnvoyConfig[string]{}, xds.Events[string]{}, l.option))
		e.ack(e.reconnect(xds.ClusterType, []string{"gone"})[0])
		eds := e.reconnect(xds.EndpointType, []string{"gone"}, "gone")
		if len(eds) != 1 || len(eds[0].GetRemovedResources()) != 0 {
			t.Fatalf("got %v, want gone kept for now", eds)
		}
		e.ack(eds[0])

		time.Sleep(30 * time.Second)
		update(src, cluster("b")) // the removals go at the next event, and the change after their ACKs
		var sent []string
		for r := e.sent(); len(r) == 1; r = e.ack(r[0]) {
			sent = append(sent, fmt.Sprint(types(r), r[0].GetRemovedResources(), deltaNames(r[0])))
		}
		want := []string{
			fmt.Sprint([]xds.TypeURL{xds.ClusterType}, []string{"gone"}, []string(nil)),
			fmt.Sprint([]xds.TypeURL{xds.EndpointType}, []string{"gone"}, []string(nil)),
			fmt.Sprint([]xds.TypeURL{xds.ClusterType}, []string(nil), []string{"b"}),
		}
		if !slices.Equal(sent, want) {
			t.Fatalf("sent %v, want %v", sent, want)
		}
		if got := l.count("removing stale resources anyway"); got != 1 || l.count("No ACK of the type updates wait for") != 0 {
			t.Fatalf("warned %d times, want once: %s", got, l.String())
		}
	})
}

func TestDelayUpdatesUntilAckOfHoldsUpdatesUntilThatAck(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		src := newSource(cluster("a"), route("r", "a"))
		cfg := xds.EnvoyConfig[string]{DelayUpdatesUntilAckOf: xds.RouteType, SubscribesTo: []xds.TypeURL{xds.ClusterType, xds.RouteType}}
		e := startDelta(t, newServer(t, src, cfg, xds.Events[string]{}))
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

func TestDelayUpdatesUntilAckOfGivesUpAfterThirtySeconds(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		src := newSource(cluster("a"))
		l := &logs{}
		e := startDelta(t, newServer(t, src, xds.EnvoyConfig[string]{DelayUpdatesUntilAckOf: xds.RouteType}, xds.Events[string]{}, l.option))
		e.ack(e.subscribe(xds.ClusterType)[0])

		time.Sleep(30*time.Second - time.Nanosecond)
		update(src, cluster("b"))
		if sent := e.sent(); len(sent) != 0 {
			t.Fatalf("got %v, want b held still", sent)
		}
		time.Sleep(time.Nanosecond)
		update(src, cluster("c"))
		if sent := e.sent(); len(sent) != 1 {
			t.Fatalf("got %v, want b and c sent anyway", sent)
		}
		if got := l.count("No ACK of the type updates wait for"); got != 1 {
			t.Fatalf("warned %d times: %s", got, l.String())
		}
	})
}

func TestDelayUpdatesUntilAckOfATypeTheEnvoyDoesNotGetIsIgnored(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		src := newSource(cluster("a"))
		cfg := xds.EnvoyConfig[string]{DelayUpdatesUntilAckOf: xds.RouteType, SubscribesTo: []xds.TypeURL{xds.ClusterType}}
		l := &logs{}
		e := startDelta(t, newServer(t, src, cfg, xds.Events[string]{}, l.option))
		e.ack(e.subscribe(xds.ClusterType)[0])

		update(src, cluster("b"))
		if sent := e.sent(); len(sent) != 1 {
			t.Fatalf("got %v, want b", sent)
		}
		if got := l.count("DelayUpdatesUntilAckOf names a type this Envoy doesn't subscribe to"); got != 1 {
			t.Fatalf("warned %d times: %s", got, l.String())
		}
	})
}

func TestDeltaLeavesUnknownTypesUnanswered(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		l := &logs{}
		e := startDelta(t, newServer(t, newSource(cluster("a")), xds.EnvoyConfig[string]{}, xds.Events[string]{}, l.option))
		if sent := e.subscribe("type.googleapis.com/envoy.config.core.v3.TypedExtensionConfig", "x"); len(sent) != 0 {
			t.Fatalf("got %v", sent)
		}
		if cds := e.subscribe(xds.ClusterType); len(cds) != 1 {
			t.Fatalf("got %v, want the stream still up", cds)
		}
		if got := l.count("Ignoring a request for an unknown type"); got != 1 {
			t.Fatalf("warned %d times", got)
		}
	})
}

func TestDeltaSendsWhatDidNotExistOnceItDoes(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		src := newSource()
		e := startDelta(t, newServer(t, src, xds.EnvoyConfig[string]{}, xds.Events[string]{}))
		eds := e.subscribe(xds.EndpointType, "a")
		if len(eds) != 1 || len(eds[0].GetResources()) != 0 || !slices.Equal(eds[0].GetRemovedResources(), []string{"a"}) {
			t.Fatalf("got %v, want a removed", eds)
		}

		update(src, cla("a", "10.0.0.1"))
		if sent := e.sent(); len(sent) != 0 {
			t.Fatalf("got %v before Envoy ACKs", sent)
		}
		if sent := e.ack(eds[0]); len(sent) != 1 || !slices.Equal(deltaNames(sent[0]), []string{"a"}) || len(sent[0].GetRemovedResources()) != 0 {
			t.Fatalf("got %v after the ACK, want a", sent)
		}
	})
}

func TestDeltaRequestNamingNothingIsNotAnswered(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		src := newSource(cluster("a"), cla("a", "10.0.0.1"))
		e := startDelta(t, newServer(t, src, xds.EnvoyConfig[string]{}, xds.Events[string]{}))
		e.ack(e.subscribe(xds.ClusterType)[0])

		for range 2 {
			if sent := e.subscribe(xds.EndpointType); len(sent) != 0 {
				t.Fatalf("got %v, want no answer", sent)
			}
		}
		update(src, cluster("b"))
		if sent := e.sent(); len(sent) != 1 {
			t.Fatalf("got %v, want b: no answer awaits an ACK", sent)
		}
	})
}

func TestDeltaWildcardRequestsAreAnsweredWithEverything(t *testing.T) {
	for _, wildcard := range [][]string{nil, {"*"}} { // legacy, and explicit
		for _, clusters := range [][]string{nil, {"a", "b"}} {
			synctest.Test(t, func(t *testing.T) {
				src := newSource()
				for _, c := range clusters {
					update(src, cluster(c))
				}
				e := startDelta(t, newServer(t, src, xds.EnvoyConfig[string]{}, xds.Events[string]{}))
				cds := e.subscribe(xds.ClusterType, wildcard...)
				if len(cds) != 1 || !slices.Equal(slices.Sorted(slices.Values(deltaNames(cds[0]))), clusters) || len(cds[0].GetRemovedResources()) != 0 {
					t.Fatalf("subscribing to %q: got %v, want %v", wildcard, cds, clusters)
				}
			})
		}
	}
}

func TestDeltaSubscribingAgainIsAnsweredAgain(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		e := startDelta(t, newServer(t, newSource(cla("a", "10.0.0.1")), xds.EnvoyConfig[string]{}, xds.Events[string]{}))
		first := e.subscribe(xds.EndpointType, "a")[0]
		again := e.subscribe(xds.EndpointType, "a")
		if len(again) != 1 || !slices.Equal(deltaNames(again[0]), []string{"a"}) || again[0].GetNonce() == first.GetNonce() {
			t.Fatalf("got %v, want a again: Envoy may have forgotten it", again)
		}
	})
}

func TestDeltaNackIsAnAnswer(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		e := startDelta(t, newServer(t, newSource(cluster("a"), route("r", "a")), xds.EnvoyConfig[string]{}, xds.Events[string]{}))
		e.nack(e.subscribe(xds.ClusterType)[0])
		if rds := e.subscribe(xds.RouteType, "r"); len(rds) != 1 {
			t.Fatalf("got %v, want the routes no longer held for the clusters", rds)
		}
	})
}

func TestDeltaSubscribingByNameToAWildcardTypeIsAnswered(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		e := startDelta(t, newServer(t, newSource(cluster("a")), xds.EnvoyConfig[string]{}, xds.Events[string]{}))
		e.ack(e.subscribe(xds.ClusterType, "*")[0])
		cds := e.subscribe(xds.ClusterType, "missing")
		if len(cds) != 1 || len(cds[0].GetResources()) != 0 || !slices.Equal(cds[0].GetRemovedResources(), []string{"missing"}) {
			t.Fatalf("got %v, want missing removed: Envoy waits to hear", cds)
		}
	})
}

func TestDeltaRemovesOnlyWhatAReconnectingEnvoyKeptAndSubscribesTo(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		e := startDelta(t, newServer(t, newSource(listener("l")), xds.EnvoyConfig[string]{}, xds.Events[string]{}))
		lds := e.reconnect(xds.ListenerType, []string{"l", "gone", "other"}, "l", "gone")
		if len(lds) != 1 || !slices.Equal(deltaNames(lds[0]), []string{"l"}) || !slices.Equal(lds[0].GetRemovedResources(), []string{"gone"}) {
			t.Fatalf("got %v, want l, and gone removed but not other", lds)
		}
		if sent := e.ack(lds[0]); len(sent) != 0 {
			t.Fatalf("got %v after the ACK", sent)
		}
	})
}

func TestDeltaUpdatesWaitForTheRemovalOfWhatAReconnectingEnvoyKept(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		src := newSource(route("r", "x"))
		cfg := xds.EnvoyConfig[string]{DelayUpdatesUntilAckOf: xds.RouteType, SubscribesTo: []xds.TypeURL{xds.ClusterType, xds.RouteType}}
		e := startDelta(t, newServer(t, src, cfg, xds.Events[string]{}))
		cds := e.reconnect(xds.ClusterType, []string{"gone"}, "*")
		if len(cds) != 1 || len(cds[0].GetResources()) != 0 || len(cds[0].GetRemovedResources()) != 0 {
			t.Fatalf("got %v, want gone kept for now", cds)
		}
		e.ack(cds[0])
		rds := e.reconnect(xds.RouteType, []string{"r"}, "r")
		if len(rds) != 1 || !slices.Equal(deltaNames(rds[0]), []string{"r"}) || len(rds[0].GetRemovedResources()) != 0 {
			t.Fatalf("got %v, want r", rds)
		}

		update(src, cluster("b"))
		if sent := e.sent(); len(sent) != 0 {
			t.Fatalf("got %v before the routes are ACKed", sent)
		}
		removal := e.ack(rds[0])
		if len(removal) != 1 || len(removal[0].GetResources()) != 0 || !slices.Equal(removal[0].GetRemovedResources(), []string{"gone"}) {
			t.Fatalf("got %v after the route ACK, want gone removed first", removal)
		}
		if sent := e.ack(removal[0]); len(sent) != 1 || !slices.Equal(deltaNames(sent[0]), []string{"b"}) || len(sent[0].GetRemovedResources()) != 0 {
			t.Fatalf("got %v after the removal's ACK, want b", sent)
		}
	})
}

func TestDeltaStaleRemovalsGoInRemoveOrderEachOnceTheLastIsAcked(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		cfg := xds.EnvoyConfig[string]{DelayUpdatesUntilAckOf: xds.RouteType,
			SubscribesTo: []xds.TypeURL{xds.ClusterType, xds.EndpointType, xds.RouteType}}
		e := startDelta(t, newServer(t, newSource(route("current", "x")), cfg, xds.Events[string]{}))
		for _, typ := range []xds.TypeURL{xds.ClusterType, xds.EndpointType} {
			sent := e.reconnect(typ, []string{"stale"}, "stale")
			if len(sent) != 1 || len(sent[0].GetResources()) != 0 || len(sent[0].GetRemovedResources()) != 0 {
				t.Fatalf("%s: got %v, want stale kept for now", typ, sent)
			}
			e.ack(sent[0])
		}
		// Nothing before routes in remove order is sent to this Envoy, so its stale routes go straight away.
		rds := e.reconnect(xds.RouteType, []string{"stale"}, "current", "stale")
		if len(rds) != 1 || !slices.Equal(deltaNames(rds[0]), []string{"current"}) || !slices.Equal(rds[0].GetRemovedResources(), []string{"stale"}) {
			t.Fatalf("got %v, want current, and stale removed", rds)
		}

		cds := e.ack(rds[0])
		if !slices.Equal(types(cds), []xds.TypeURL{xds.ClusterType}) || !slices.Equal(cds[0].GetRemovedResources(), []string{"stale"}) {
			t.Fatalf("got %v after the route ACK, want the stale cluster removed", cds)
		}
		if sent := e.subscribe(xds.ClusterType); len(sent) != 0 {
			t.Fatalf("got %v for a request that isn't an ACK", sent)
		}
		if eds := e.ack(cds[0]); !slices.Equal(types(eds), []xds.TypeURL{xds.EndpointType}) || !slices.Equal(eds[0].GetRemovedResources(), []string{"stale"}) {
			t.Fatalf("got %v after the cluster ACK, want the stale endpoints removed", eds)
		}
	})
}

func TestDeltaResourcesHaveTheVersionOfTheirResponse(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		e := startDelta(t, newServer(t, newSource(cla("a", "10.0.0.1")), xds.EnvoyConfig[string]{}, xds.Events[string]{}))
		eds := e.subscribe(xds.EndpointType, "a")[0]
		if v := eds.GetResources()[0].GetVersion(); v != eds.GetSystemVersionInfo() {
			t.Fatalf("resource version %q, response version %q", v, eds.GetSystemVersionInfo())
		}
	})
}

func TestBuildersGivingOnlyWhatWasAskedForAreNotWarnedAbout(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		src := newSource(cla("a", "10.0.0.1"), cla("b", "10.0.0.2"))
		l := &logs{}
		e := startDelta(t, newServer(t, src, xds.EnvoyConfig[string]{}, xds.Events[string]{}, l.option))
		e.ack(e.subscribe(xds.EndpointType, "a", "b")[0])
		edit(src, func(e *snapshot.Editor) {
			e.Put(cla("a", "10.0.0.3"))
			e.Remove(xds.EndpointType, "b")
		})
		for sent := e.sent(); len(sent) > 0; sent = e.ack(sent[0]) {
		}
		if l.Len() != 0 {
			t.Fatalf("logged %s", l.String())
		}
	})
}
