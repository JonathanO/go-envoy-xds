package xds_test

import (
	"bytes"
	"context"
	"log/slog"
	"slices"
	"strings"
	"testing"
	"testing/synctest"

	corev3 "github.com/envoyproxy/go-control-plane/envoy/config/core/v3"

	xds "github.com/JonathanO/go-envoy-xds"
)

// leaky serves endpoints, always including some nobody asked for.
type leaky struct{}

func (leaky) Type() xds.TypeURL { return xds.EndpointType }
func (leaky) AddOrder(int, func(string) bool, string) xds.Response {
	return xds.Response{Updates: []xds.Resource{{Name: "unasked", Message: cla("unasked", "10.0.0.9")}}}
}
func (leaky) RemoveOrder(int, func(string) bool, string) xds.Response {
	return xds.Response{Removes: []string{"unasked"}}
}
func (leaky) ResourcesAddOrder(int, func(string) bool, string) []xds.Resource {
	return []xds.Resource{{Name: "unasked", Message: cla("unasked", "10.0.0.9")}}
}
func (leaky) ResourcesRemoveOrder(u int, s func(string) bool, d string) []xds.Resource {
	return leaky{}.ResourcesAddOrder(u, s, d)
}

// counter is a Source of ints, each update the next.
type counter struct{ deliver func(int) }

func (c *counter) Subscribe(_ string, deliver func(int)) (int, func()) {
	c.deliver = deliver
	return 0, func() {}
}

func TestBuildersReturningWhatEnvoyDidNotAskForAreWarnedAbout(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var log bytes.Buffer
		src := &counter{}
		srv, err := xds.NewServer(xds.ServerConfig[int, string]{
			Source:   src,
			Builders: []xds.ConfigBuilder[int, string]{leaky{}},
			ConfigFor: func(context.Context, *corev3.Node) (xds.EnvoyConfig[string], error) {
				return xds.EnvoyConfig[string]{}, nil
			},
			Logger: slog.New(slog.NewTextHandler(&log, nil)),
		})
		if err != nil {
			t.Fatal(err)
		}
		e := &deltaEnvoy{start(t, srv.DeltaAggregatedResources)}
		e.ack(e.subscribe(xds.EndpointType, "a")[0])
		src.deliver(1)
		e.ack(e.sent()[0])
		e.sent()
		if got := strings.Count(log.String(), "didn't ask for"); got != 3 {
			t.Fatalf("got %d warnings, want one per call: %s", got, log.String())
		}
	})
}

func TestTypesEnvoyHasNotAskedForAreNotBuilt(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		src := &counter{}
		srv, err := xds.NewServer(xds.ServerConfig[int, string]{
			Source:   src,
			Builders: []xds.ConfigBuilder[int, string]{leaky{}},
			ConfigFor: func(context.Context, *corev3.Node) (xds.EnvoyConfig[string], error) {
				return xds.EnvoyConfig[string]{}, nil
			},
			Logger: slog.New(slog.DiscardHandler),
		})
		if err != nil {
			t.Fatal(err)
		}
		e := &deltaEnvoy{start(t, srv.DeltaAggregatedResources)}
		e.ack(e.subscribe(xds.ClusterType)[0]) // which nothing serves
		src.deliver(1)
		if sent := e.sent(); len(sent) != 0 {
			t.Fatalf("got %v, want nothing: Envoy hasn't asked for endpoints", sent)
		}
	})
}

// twoFaced serves cluster b and its endpoints once the counter is past 0. Its add round has b's endpoints at
// 10.0.0.2, but subscriptions are answered with them at 10.0.0.1.
type twoFaced struct{ typ xds.TypeURL }

func (b twoFaced) b(address string) []xds.Resource {
	if b.typ == xds.ClusterType {
		return []xds.Resource{{Name: "b", Message: cluster("b")}}
	}
	return []xds.Resource{{Name: "b", Message: cla("b", address)}}
}

func (b twoFaced) Type() xds.TypeURL { return b.typ }
func (b twoFaced) AddOrder(int, func(string) bool, string) xds.Response {
	return xds.Response{Updates: b.b("10.0.0.2")}
}
func (twoFaced) RemoveOrder(int, func(string) bool, string) xds.Response { return xds.Response{} }
func (b twoFaced) ResourcesAddOrder(u int, _ func(string) bool, _ string) []xds.Resource {
	if u == 0 {
		return nil
	}
	return b.b("10.0.0.1")
}
func (b twoFaced) ResourcesRemoveOrder(u int, s func(string) bool, d string) []xds.Resource {
	return b.ResourcesAddOrder(u, s, d)
}

func TestDeltaChangeSendsWhatASubscriptionWasAnsweredWithOtherwise(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		src := &counter{}
		srv, err := xds.NewServer(xds.ServerConfig[int, string]{
			Source:   src,
			Builders: []xds.ConfigBuilder[int, string]{twoFaced{xds.ClusterType}, twoFaced{xds.EndpointType}},
			ConfigFor: func(context.Context, *corev3.Node) (xds.EnvoyConfig[string], error) {
				return xds.EnvoyConfig[string]{}, nil
			},
			Logger: slog.New(slog.DiscardHandler),
		})
		if err != nil {
			t.Fatal(err)
		}
		e := &deltaEnvoy{start(t, srv.DeltaAggregatedResources)}
		e.ack(e.subscribe(xds.ClusterType)[0])

		// A change adds cluster b. Envoy asks for b's endpoints before the change gets to EDS.
		src.deliver(1)
		cds := e.sent()[0]
		e.ack(e.subscribe(xds.EndpointType, "b")[0])
		eds := e.ack(cds)
		if len(eds) != 1 || len(eds[0].GetResources()) != 1 || address(t, eds[0].GetResources()[0].GetResource()) != "10.0.0.2" {
			t.Fatalf("got %v after the cluster ACK, want b's endpoints as the change has them", eds)
		}
	})
}

// reversed serves clusters b and a, in that order, from every call, whether or not anything changed.
type reversed struct{}

func (reversed) Type() xds.TypeURL { return xds.ClusterType }
func (reversed) AddOrder(int, func(string) bool, string) xds.Response {
	return xds.Response{Updates: reversed{}.ResourcesAddOrder(0, nil, "")}
}
func (reversed) RemoveOrder(int, func(string) bool, string) xds.Response { return xds.Response{} }
func (reversed) ResourcesAddOrder(int, func(string) bool, string) []xds.Resource {
	return []xds.Resource{{Name: "b", Message: cluster("b")}, {Name: "a", Message: cluster("a")}}
}
func (reversed) ResourcesRemoveOrder(u int, s func(string) bool, d string) []xds.Resource {
	return reversed{}.ResourcesAddOrder(u, s, d)
}

func reversedServer(t *testing.T, src *counter) *xds.Server[int, string] {
	t.Helper()
	srv, err := xds.NewServer(xds.ServerConfig[int, string]{
		Source:   src,
		Builders: []xds.ConfigBuilder[int, string]{reversed{}},
		ConfigFor: func(context.Context, *corev3.Node) (xds.EnvoyConfig[string], error) {
			return xds.EnvoyConfig[string]{}, nil
		},
		Logger: slog.New(slog.DiscardHandler),
	})
	if err != nil {
		t.Fatal(err)
	}
	return srv
}

// The Server doesn't compare updates: what to send is the builders' call.
func TestAnUpdateEqualToTheLastStillGoesThroughTheBuilders(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		src := &counter{}
		e := &deltaEnvoy{start(t, reversedServer(t, src).DeltaAggregatedResources)}
		e.ack(e.subscribe(xds.ClusterType)[0])
		for range 2 {
			src.deliver(0) // as Subscribe gave
			sent := e.sent()
			if len(sent) != 1 || !slices.Equal(deltaNames(sent[0]), []string{"b", "a"}) {
				t.Fatalf("got %v, want what the builder sent again", sent)
			}
			e.ack(sent[0])
		}
	})
}

func TestSotwListsResourcesByName(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		src := &counter{}
		e := &sotwEnvoy{start(t, reversedServer(t, src).StreamAggregatedResources)}
		answer := e.request(xds.ClusterType, nil)
		if len(answer) != 1 || !slices.Equal(names(t, answer[0]), []string{"a", "b"}) {
			t.Fatalf("answered %v, want a then b", answer)
		}
		e.request(xds.ClusterType, answer[0])
		src.deliver(1)
		push := e.sent()
		if len(push) != 1 || !slices.Equal(names(t, push[0]), []string{"a", "b"}) {
			t.Fatalf("pushed %v, want a then b", push)
		}
	})
}
