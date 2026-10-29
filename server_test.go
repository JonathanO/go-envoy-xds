package xds_test

import (
	"context"
	"errors"
	"log/slog"
	"testing"
	"testing/synctest"

	corev3 "github.com/envoyproxy/go-control-plane/envoy/config/core/v3"
	discoveryv3 "github.com/envoyproxy/go-control-plane/envoy/service/discovery/v3"

	xds "github.com/JonathanO/go-envoy-xds"
	"github.com/JonathanO/go-envoy-xds/snapshot"
)

func TestNewServerRejectsIncompleteConfig(t *testing.T) {
	configFor := func(context.Context, *corev3.Node) (xds.EnvoyConfig[string], error) {
		return xds.EnvoyConfig[string]{}, nil
	}
	for name, c := range map[string]xds.ServerConfig[*snapshot.Update, string]{
		"no source":     {ConfigFor: configFor},
		"no ConfigFor":  {Source: newSource()},
		"unknown type":  {Source: newSource(), ConfigFor: configFor, Builders: snapshot.Builders[string]("type.googleapis.com/x")},
		"repeated type": {Source: newSource(), ConfigFor: configFor, Builders: snapshot.Builders[string](xds.ClusterType, xds.ClusterType)},
	} {
		if _, err := xds.NewServer(c); err == nil {
			t.Errorf("%s: no error", name)
		}
	}
}

func TestEventsFollowTheStream(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var events []string
		var client *xds.Client[string]
		e := startSotw(t, newServer(t, newSource(), xds.EnvoyConfig[string]{Details: "d"}, xds.Events[string]{
			Connected:    func(c *xds.Client[string]) { client = c; events = append(events, "connected") },
			Disconnected: func(c *xds.Client[string]) { events = append(events, "disconnected") },
		}))
		e.request(xds.ClusterType, nil)
		if client.Node.GetId() != "envoy" || client.Config.Details != "d" {
			t.Fatalf("client is %v", client)
		}

		client.Resync()
		if err := <-e.ended; err != nil {
			t.Fatalf("resync ended the stream with %v, want OK so Envoy reconnects", err)
		}
		synctest.Wait()
		if len(events) != 2 || events[1] != "disconnected" {
			t.Fatalf("events %v", events)
		}
		client.Resync() // after the stream's gone, nothing happens
	})
}

func TestAnErrorFromConfigForEndsTheStream(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		denied := errors.New("denied")
		srv, err := xds.NewServer(xds.ServerConfig[*snapshot.Update, string]{
			Source: newSource(),
			ConfigFor: func(context.Context, *corev3.Node) (xds.EnvoyConfig[string], error) {
				return xds.EnvoyConfig[string]{}, denied
			},
		})
		if err != nil {
			t.Fatal(err)
		}
		e := startSotw(t, srv)
		e.stream.reqs <- nil
		if err := <-e.ended; !errors.Is(err, denied) {
			t.Fatalf("got %v", err)
		}
	})
}

func TestAnEnvoyClosingItsStreamEndsIt(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		disconnected := 0
		w, m := &watched{}, &metrics{}
		e := startDelta(t, newServer(t, newSource(), xds.EnvoyConfig[string]{}, xds.Events[string]{
			Disconnected: func(*xds.Client[string]) { disconnected++ },
		}, w.option, m.option))
		e.subscribe(xds.ClusterType)
		close(e.stream.reqs)
		if err := <-e.ended; err != nil {
			t.Fatalf("got %v", err)
		}
		if disconnected != 1 || w.cancelled != 1 || m.calls[len(m.calls)-1] != "closed" {
			t.Fatalf("disconnected %d times, unsubscribed %d, metrics %v", disconnected, w.cancelled, m.calls)
		}
	})
}

func TestAStreamThatEndsBeforeARequestIsNotDisconnected(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		disconnected := false
		e := startDelta(t, newServer(t, newSource(), xds.EnvoyConfig[string]{}, xds.Events[string]{
			Disconnected: func(*xds.Client[string]) { disconnected = true },
		}))
		close(e.stream.reqs)
		<-e.ended
		if disconnected {
			t.Fatal("disconnected an Envoy that never connected")
		}
	})
}

func TestAFailedSendEndsTheStream(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		broken := errors.New("broken")
		e := startDelta(t, newServer(t, newSource(), xds.EnvoyConfig[string]{}, xds.Events[string]{}))
		e.stream.fail = broken
		e.stream.reqs <- &discoveryv3.DeltaDiscoveryRequest{Node: node, TypeUrl: string(xds.ClusterType)}
		if err := <-e.ended; !errors.Is(err, broken) {
			t.Fatalf("got %v", err)
		}
	})
}

func TestServersLogToTheDefaultLogger(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		l := &logs{}
		defer slog.SetDefault(slog.Default())
		slog.SetDefault(slog.New(slog.NewTextHandler(l, nil)))
		e := startDelta(t, newServer(t, newSource(), xds.EnvoyConfig[string]{}, xds.Events[string]{}, func(c *serverConfig) { c.Logger = nil }))
		e.subscribe("type.googleapis.com/x")
		if l.count("unknown type") != 1 {
			t.Fatalf("logged %q", l.String())
		}
	})
}

func TestAResourceThatCannotBeSentEndsTheStream(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		srv := newServer(t, newSource(cluster("bad\xff")), xds.EnvoyConfig[string]{}, xds.Events[string]{})
		sotw, delta := startSotw(t, srv), startDelta(t, srv)
		sotw.stream.reqs <- &discoveryv3.DiscoveryRequest{Node: node, TypeUrl: string(xds.ClusterType)}
		delta.stream.reqs <- &discoveryv3.DeltaDiscoveryRequest{Node: node, TypeUrl: string(xds.ClusterType)}
		if err := <-sotw.ended; err == nil {
			t.Error("the SotW stream ended OK")
		}
		if err := <-delta.ended; err == nil {
			t.Error("the delta stream ended OK")
		}
	})
}
