package xds_test

import (
	"fmt"
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

func TestSotwIgnoresAnswersToEarlierResponses(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		src := newSource(cluster("a"))
		l := &logs{}
		e := startSotw(t, newServer(t, src, xds.EnvoyConfig[string]{}, xds.Events[string]{}, l.option))
		first := e.request(xds.ClusterType, nil)[0]
		e.request(xds.ClusterType, first)
		update(src, cluster("b"))
		second := e.sent()[0]

		e.request(xds.ClusterType, first, "a") // late, from when Envoy wanted a alone
		update(src, cluster("c"))
		if sent := e.sent(); len(sent) != 0 {
			t.Fatalf("got %v, want nothing until Envoy answers %s", sent, second.GetNonce())
		}
		if sent := e.request(xds.ClusterType, second); len(sent) != 1 || !slices.Equal(names(t, sent[0]), []string{"a", "b", "c"}) {
			t.Fatalf("got %v after the ACK, want every cluster", sent)
		}
		if got := l.count("Ignoring a request answering an earlier response"); got != 1 {
			t.Fatalf("logged %d", got)
		}
	})
}

func TestSotwSubscribingToMoreKeepsWhatItHad(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		e := startSotw(t, newServer(t, newSource(cla("a", "10.0.0.1"), cla("b", "10.0.0.2")), xds.EnvoyConfig[string]{}, xds.Events[string]{}))
		eds := e.request(xds.EndpointType, nil, "a")[0]
		if sent := e.request(xds.EndpointType, eds, "a", "b"); len(sent) != 1 || !slices.Equal(names(t, sent[0]), []string{"a", "b"}) {
			t.Fatalf("got %v, want a and b", sent)
		}
	})
}

func TestSotwSubscriptionDuringTheAddRoundGetsItsTypesAddsWhichAreNotSentAgain(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		src := newSource(cluster("a"), cla("a", "10.0.0.1"))
		e := startSotw(t, newServer(t, src, xds.EnvoyConfig[string]{}, xds.Events[string]{}))
		e.request(xds.ClusterType, e.request(xds.ClusterType, nil)[0])
		eds := e.request(xds.EndpointType, nil, "a")[0]
		e.request(xds.EndpointType, eds, "a")

		update(src, cluster("b"), cla("a", "10.0.0.3"), cla("b", "10.0.0.2"))
		cds := e.sent()[0]
		sent := e.request(xds.EndpointType, eds, "a", "b")
		if len(sent) != 1 || !slices.Equal(names(t, sent[0]), []string{"a", "b"}) || address(t, sent[0].GetResources()[0]) != "10.0.0.3" {
			t.Fatalf("got %v, want the change's endpoints too", sent)
		}
		e.request(xds.EndpointType, sent[0], "a", "b")
		if sent := e.request(xds.ClusterType, cds); len(sent) != 0 {
			t.Fatalf("got %v after the cluster ACK, want nothing more", sent)
		}
	})
}

func TestSotwSubscriptionDuringTheRemoveRoundGetsItsTypesRemovalsWhichAreNotSentAgain(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		src := newSource(cluster("a"), cla("a", "10.0.0.1"), cla("c", "10.0.0.2"), route("r", "a"))
		e := startSotw(t, newServer(t, src, xds.EnvoyConfig[string]{}, xds.Events[string]{}))
		e.request(xds.ClusterType, e.request(xds.ClusterType, nil)[0])
		e.request(xds.RouteType, e.request(xds.RouteType, nil, "r")[0], "r")
		eds := e.request(xds.EndpointType, nil, "a")[0]
		e.request(xds.EndpointType, eds, "a")

		edit(src, func(e *snapshot.Editor) {
			e.Remove(xds.RouteType, "r")
			e.Remove(xds.EndpointType, "a")
		})
		rds := e.sent()[0]
		sent := e.request(xds.EndpointType, eds, "a", "c")
		if len(sent) != 1 || !slices.Equal(names(t, sent[0]), []string{"c"}) {
			t.Fatalf("got %v, want c, and a's endpoints gone", sent)
		}
		e.request(xds.EndpointType, sent[0], "a", "c")
		if sent := e.request(xds.RouteType, rds, "r"); len(sent) != 0 {
			t.Fatalf("got %v after the route ACK, want nothing more", sent)
		}
	})
}

func TestSotwNackStopsTheRestOfTheChangeInAnswersToo(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		src := newSource(cluster("a"), cla("a", "10.0.0.1"))
		e := startSotw(t, newServer(t, src, xds.EnvoyConfig[string]{}, xds.Events[string]{}))
		e.request(xds.ClusterType, e.request(xds.ClusterType, nil)[0])
		eds := e.request(xds.EndpointType, nil, "a")[0]
		e.request(xds.EndpointType, eds, "a")

		update(src, cluster("b"), cla("a", "10.0.0.2"))
		cds := e.sent()[0]
		e.send(&discoveryv3.DiscoveryRequest{Node: node, TypeUrl: string(xds.ClusterType), ResponseNonce: cds.GetNonce(),
			VersionInfo: "1", ErrorDetail: &statuspb.Status{Message: "no"}})
		sent := e.request(xds.EndpointType, eds, "a", "b")
		if len(sent) != 1 || address(t, sent[0].GetResources()[0]) != "10.0.0.1" {
			t.Fatalf("got %v, want a where it was", sent)
		}
	})
}

// counting counts the RemoveOrder calls of the builder it wraps.
type counting struct {
	xds.ConfigBuilder[*snapshot.Update, string]
	removeOrders *int
}

func (c counting) RemoveOrder(u *snapshot.Update, subscribed func(string) bool, d string) xds.Response {
	*c.removeOrders++
	return c.ConfigBuilder.RemoveOrder(u, subscribed, d)
}

func TestSotwAsksForEachRoundOnce(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		src := newSource(cla("a", "10.0.0.1"), cla("b", "10.0.0.2"))
		n := 0
		e := startSotw(t, newServer(t, src, xds.EnvoyConfig[string]{}, xds.Events[string]{}, func(c *serverConfig) {
			c.Builders = []xds.ConfigBuilder[*snapshot.Update, string]{counting{snapshot.Builders[string](xds.EndpointType)[0], &n}}
		}))
		e.request(xds.EndpointType, e.request(xds.EndpointType, nil, "a")[0], "a")
		update(src, cla("a", "10.0.0.3"))
		push := e.sent()[0]
		e.request(xds.EndpointType, push, "a")
		e.request(xds.EndpointType, push, "a", "b")
		if n != 1 {
			t.Fatalf("asked for the change's removals %d times", n)
		}
	})
}

func TestSotwSubscriptionAfterAChangeGetsNoneOfWhatItRemoved(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		src := newSource(cla("a", "10.0.0.1"), cla("gone", "10.0.0.2"))
		e := startSotw(t, newServer(t, src, xds.EnvoyConfig[string]{}, xds.Events[string]{}))
		eds := e.request(xds.EndpointType, nil, "a")[0]
		e.request(xds.EndpointType, eds, "a")
		edit(src, func(e *snapshot.Editor) { e.Remove(xds.EndpointType, "gone") })
		if sent := e.request(xds.EndpointType, eds, "a", "gone"); len(sent) != 1 || !slices.Equal(names(t, sent[0]), []string{"a"}) {
			t.Fatalf("got %v, want a alone", sent)
		}
	})
}

func TestSotwRequestIsAnsweredWithWhatItNamesAndAwaitsItsAck(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		src := newSource(cla("a", "10.0.0.1"), cla("b", "10.0.0.2"))
		e := startSotw(t, newServer(t, src, xds.EnvoyConfig[string]{}, xds.Events[string]{}))
		eds := e.request(xds.EndpointType, nil, "a")
		if len(eds) != 1 || !slices.Equal(names(t, eds[0]), []string{"a"}) {
			t.Fatalf("got %v, want a alone", eds)
		}

		update(src, cla("a", "10.0.0.3"))
		if sent := e.sent(); len(sent) != 0 {
			t.Fatalf("got %v before Envoy ACKs", sent)
		}
		if sent := e.request(xds.EndpointType, eds[0], "a"); len(sent) != 1 || address(t, sent[0].GetResources()[0]) != "10.0.0.3" {
			t.Fatalf("got %v after the ACK, want a moved", sent)
		}
	})
}

func TestSotwNackIsAnAnswer(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		e := startSotw(t, newServer(t, newSource(cluster("a"), route("r", "a")), xds.EnvoyConfig[string]{}, xds.Events[string]{}))
		cds := e.request(xds.ClusterType, nil)[0]
		e.send(&discoveryv3.DiscoveryRequest{Node: node, TypeUrl: string(xds.ClusterType), ResponseNonce: cds.GetNonce(),
			ErrorDetail: &statuspb.Status{Message: "no"}})
		if rds := e.request(xds.RouteType, nil, "r"); len(rds) != 1 {
			t.Fatalf("got %v, want the routes no longer held for the clusters", rds)
		}
	})
}

func TestSotwLegacyWildcardRequestIsAnsweredWithEverything(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		e := startSotw(t, newServer(t, newSource(cluster("a"), cluster("b")), xds.EnvoyConfig[string]{}, xds.Events[string]{}))
		cds := e.request(xds.ClusterType, nil)
		if len(cds) != 1 || !slices.Equal(names(t, cds[0]), []string{"a", "b"}) {
			t.Fatalf("got %v, want every cluster", cds)
		}
		if sent := e.request(xds.ClusterType, cds[0], "*"); len(sent) != 0 {
			t.Fatalf("got %v, want no answer: the explicit wildcard is the same subscription", sent)
		}
	})
}

func TestSotwAnswerWithAnotherVersionIsNotAnAck(t *testing.T) {
	for _, version := range []string{"", "v-abc", "1"} { // none, another control plane's, and the one before
		synctest.Test(t, func(t *testing.T) {
			src, l := newSource(cluster("a")), &logs{}
			e := startSotw(t, newServer(t, src, xds.EnvoyConfig[string]{}, xds.Events[string]{}, l.option))
			e.request(xds.ClusterType, e.request(xds.ClusterType, nil)[0])
			update(src, cluster("b"))
			push := e.sent()[0]

			e.send(&discoveryv3.DiscoveryRequest{Node: node, TypeUrl: string(xds.ClusterType), VersionInfo: version, ResponseNonce: push.GetNonce()})
			update(src, cluster("c"))
			if sent := e.sent(); len(sent) != 0 {
				t.Fatalf("answered with version %q: got %v, want c held until an ACK", version, sent)
			}
			logged := fmt.Sprintf(`level=ERROR msg="Envoy rejected a version and rolled back" node=envoy type=%s version=%s to=%s`, xds.ClusterType, push.GetVersionInfo(), version)
			if version == "" {
				logged = fmt.Sprintf(`level=ERROR msg="Envoy rejected a version and has none to roll back to" node=envoy type=%s version=%s`, xds.ClusterType, push.GetVersionInfo())
			}
			if got := l.count(logged); got != 1 {
				t.Fatalf("answered with version %q: logged %q", version, l.String())
			}
		})
	}
}

// Envoy answering with another version rejects the push, and ACKing the next one accepts it.
func TestSotwAckAfterAnAnswerWithAnotherVersionLetsChangesGo(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		src := newSource(cla("a", "10.0.0.1"), cla("b", "10.0.0.2"))
		e := startSotw(t, newServer(t, src, xds.EnvoyConfig[string]{}, xds.Events[string]{}))
		first := e.request(xds.EndpointType, nil, "a")[0]
		reject := func(names ...string) []*discoveryv3.DiscoveryResponse {
			return e.send(&discoveryv3.DiscoveryRequest{Node: node, TypeUrl: string(xds.EndpointType), ResponseNonce: first.GetNonce(), ResourceNames: names})
		}

		reject("a")
		update(src, cla("a", "10.0.0.3"))
		if sent := e.sent(); len(sent) != 0 {
			t.Fatalf("got %v, want the change held: Envoy didn't accept %s", sent, first.GetVersionInfo())
		}
		second := reject("a", "b")
		if len(second) != 1 || !slices.Equal(names(t, second[0]), []string{"a", "b"}) || second[0].GetVersionInfo() == first.GetVersionInfo() {
			t.Fatalf("got %v, want a and b pushed again in another version", second)
		}
		if sent := e.request(xds.EndpointType, second[0], "a", "b"); len(sent) != 1 || address(t, sent[0].GetResources()[0]) != "10.0.0.3" {
			t.Fatalf("got %v after the ACK, want a moved", sent)
		}
	})
}

func TestSotwLeavingTheWildcardIsAnswered(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		e := startSotw(t, newServer(t, newSource(cluster("a"), cluster("b")), xds.EnvoyConfig[string]{}, xds.Events[string]{}))
		cds := e.request(xds.ClusterType, nil)[0]
		e.request(xds.ClusterType, cds)
		if sent := e.request(xds.ClusterType, cds, "a"); len(sent) != 1 || !slices.Equal(names(t, sent[0]), []string{"a"}) {
			t.Fatalf("got %v, want a alone", sent)
		}
	})
}
