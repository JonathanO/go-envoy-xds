package xds_test

import (
	"bytes"
	"context"
	"log/slog"
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
