package xds_test

import (
	"context"
	"errors"
	"testing"
	"testing/synctest"

	corev3 "github.com/envoyproxy/go-control-plane/envoy/config/core/v3"

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
