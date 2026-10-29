package xds_test

import (
	"slices"
	"testing"
	"testing/synctest"

	discoveryv3 "github.com/envoyproxy/go-control-plane/envoy/service/discovery/v3"
	statuspb "google.golang.org/genproto/googleapis/rpc/status"

	xds "github.com/JonathanO/go-envoy-xds"
	"github.com/JonathanO/go-envoy-xds/snapshot"
)

func TestSotwAnswersEveryFirstRequestEvenWithNothingToSend(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		e := startSotw(t, newServer(t, newSource(), xds.EnvoyConfig[string]{}, xds.Events[string]{}))

		cds := e.request(xds.ClusterType, nil)
		if len(cds) != 1 || len(cds[0].GetResources()) != 0 {
			t.Fatalf("got %v, want one empty response: Envoy waits for it", cds)
		}
	})
}

func TestSotwRoutesToANewClusterWaitForItsAck(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		src := newSource(cluster("a"), route("r", "a"))
		e := startSotw(t, newServer(t, src, xds.EnvoyConfig[string]{}, xds.Events[string]{}))
		cds := e.request(xds.ClusterType, nil)[0]
		e.request(xds.ClusterType, cds)
		rds := e.request(xds.RouteType, nil, "r")[0]
		e.request(xds.RouteType, rds, "r")

		update(src, cluster("b"), route("r", "b"))
		sent := e.sent()
		if !slices.Equal(types(sent), []xds.TypeURL{xds.ClusterType}) || !slices.Equal(names(t, sent[0]), []string{"a", "b"}) {
			t.Fatalf("got %v, want the clusters alone first", sent)
		}
		sent = e.request(xds.ClusterType, sent[0])
		if !slices.Equal(types(sent), []xds.TypeURL{xds.RouteType}) {
			t.Fatalf("got %v after the cluster ACK, want the route", sent)
		}
	})
}

func TestSotwRemovesAClusterOnceTheRouteStopsUsingIt(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		src := newSource(cluster("a"), cluster("b"), route("r", "a"))
		e := startSotw(t, newServer(t, src, xds.EnvoyConfig[string]{}, xds.Events[string]{}))
		e.request(xds.ClusterType, e.request(xds.ClusterType, nil)[0])
		e.request(xds.RouteType, e.request(xds.RouteType, nil, "r")[0], "r")

		edit(src, func(e *snapshot.Editor) {
			e.Put(route("r", "b"))
			e.Remove(xds.ClusterType, "a")
		})
		rds := e.sent()
		if !slices.Equal(types(rds), []xds.TypeURL{xds.RouteType}) {
			t.Fatalf("got %v, want the route first", rds)
		}
		cds := e.request(xds.RouteType, rds[0], "r")
		if !slices.Equal(types(cds), []xds.TypeURL{xds.ClusterType}) || !slices.Equal(names(t, cds[0]), []string{"b"}) {
			t.Fatalf("got %v after the route ACK, want clusters without a", cds)
		}
	})
}

func TestSotwUnsubscribingIsNotAnswered(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		src := newSource(cla("a", "10.0.0.1"), cla("b", "10.0.0.2"))
		e := startSotw(t, newServer(t, src, xds.EnvoyConfig[string]{}, xds.Events[string]{}))
		eds := e.request(xds.EndpointType, nil, "a", "b")[0]

		if sent := e.request(xds.EndpointType, eds, "a"); len(sent) != 0 {
			t.Fatalf("got %v, want no answer: Envoy drops b itself", sent)
		}
		update(src, cla("a", "10.0.0.3"))
		if sent := e.sent(); len(sent) != 1 || !slices.Equal(names(t, sent[0]), []string{"a"}) {
			t.Fatalf("got %v, want a alone", sent)
		}
	})
}

func TestSotwIgnoresAnswersToEarlierResponses(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		src := newSource(cluster("a"))
		e := startSotw(t, newServer(t, src, xds.EnvoyConfig[string]{}, xds.Events[string]{}))
		first := e.request(xds.ClusterType, nil)[0]
		e.request(xds.ClusterType, first)
		update(src, cluster("b"))
		second := e.sent()[0]

		e.request(xds.ClusterType, first) // late
		update(src, cluster("c"))
		if sent := e.sent(); len(sent) != 0 {
			t.Fatalf("got %v, want nothing until Envoy answers %s", sent, second.GetNonce())
		}
		if sent := e.request(xds.ClusterType, second); len(sent) != 1 {
			t.Fatalf("got %v after the ACK, want c", sent)
		}
	})
}

func TestSotwNackFreezesTheStreamButSubscriptionsAreStillAnswered(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		src := newSource(cluster("a"), cla("a", "10.0.0.1"))
		var nacked []xds.TypeURL
		e := startSotw(t, newServer(t, src, xds.EnvoyConfig[string]{}, xds.Events[string]{
			Nacked: func(_ *xds.Client[string], typ xds.TypeURL, _ *statuspb.Status) { nacked = append(nacked, typ) },
		}))
		e.request(xds.ClusterType, e.request(xds.ClusterType, nil)[0])
		update(src, cluster("bad"))
		bad := e.sent()[0]

		e.send(&discoveryv3.DiscoveryRequest{Node: node, TypeUrl: string(xds.ClusterType), ResponseNonce: bad.GetNonce(),
			VersionInfo: "1", ErrorDetail: &statuspb.Status{Message: "no"}})
		if !slices.Equal(nacked, []xds.TypeURL{xds.ClusterType}) {
			t.Errorf("Nacked got %v", nacked)
		}
		update(src, cluster("c"))
		if sent := e.sent(); len(sent) != 0 {
			t.Fatalf("got %v, want no more changes after a NACK", sent)
		}
		if eds := e.request(xds.EndpointType, nil, "a"); len(eds) != 1 {
			t.Fatalf("got %v, want a subscription still answered", eds)
		}
	})
}

func TestSotwSubscriptionDuringAChangeGetsItsTypesPartOfItWhichIsNotSentAgain(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		src := newSource(cluster("a"), cla("a", "10.0.0.1"))
		e := startSotw(t, newServer(t, src, xds.EnvoyConfig[string]{}, xds.Events[string]{}))
		e.request(xds.ClusterType, e.request(xds.ClusterType, nil)[0])

		update(src, cluster("b"), cla("b", "10.0.0.2"))
		cds := e.sent()[0]
		eds := e.request(xds.EndpointType, nil, "a", "b")
		if len(eds) != 1 || !slices.Equal(names(t, eds[0]), []string{"a", "b"}) {
			t.Fatalf("got %v, want the change's endpoints too", eds)
		}
		e.request(xds.EndpointType, eds[0], "a", "b")
		if sent := e.request(xds.ClusterType, cds); len(sent) != 0 {
			t.Fatalf("got %v after the cluster ACK, want nothing more", sent)
		}
	})
}
