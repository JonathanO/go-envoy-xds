package xds_test

import (
	"context"
	"errors"
	"log/slog"
	"slices"
	"testing"
	"testing/synctest"

	corev3 "github.com/envoyproxy/go-control-plane/envoy/config/core/v3"
	discoveryv3 "github.com/envoyproxy/go-control-plane/envoy/service/discovery/v3"
	statuspb "google.golang.org/genproto/googleapis/rpc/status"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

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
			Nacked: func(c *xds.Client[string], typ xds.TypeURL, detail *statuspb.Status) {
				if c != client || typ != xds.ClusterType {
					t.Errorf("Nacked(%v, %s)", c, typ)
				}
				events = append(events, "nacked: "+detail.GetMessage())
			},
		}))
		cds := e.request(xds.ClusterType, nil)[0]
		if client.Node.GetId() != "envoy" || client.Config.Details != "d" {
			t.Fatalf("client is %v", client)
		}
		e.send(&discoveryv3.DiscoveryRequest{Node: node, TypeUrl: string(xds.ClusterType), ResponseNonce: cds.GetNonce(),
			ErrorDetail: &statuspb.Status{Message: "no"}})

		client.Resync()
		if err := <-e.ended; err != nil {
			t.Fatalf("resync ended the stream with %v, want OK so Envoy reconnects", err)
		}
		e.stream.reqs <- &discoveryv3.DiscoveryRequest{Node: node, TypeUrl: string(xds.EndpointType)} // sent before Envoy saw it end
		if sent := e.sent(); len(sent) != 0 {
			t.Fatalf("got %v after the resync", sent)
		}
		if want := []string{"connected", "nacked: no", "disconnected"}; !slices.Equal(events, want) {
			t.Fatalf("events %v, want %v", events, want)
		}
		client.Resync() // after the stream's gone, nothing happens
	})
}

func TestTheSourceIsSubscribedToWithTheEnvoysDetails(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		src := snapshot.New[string, string, string](func(details string) string { return details })
		update(src, cluster("a"))
		src.Update(func(c *snapshot.Change[string, string]) { c.SetView("d", "g") })
		e := startSotw(t, newServer(t, src, xds.EnvoyConfig[string]{Details: "d"}, xds.Events[string]{}))
		if cds := e.request(xds.ClusterType, nil); len(cds) != 1 || !slices.Equal(names(t, cds[0]), []string{"a"}) {
			t.Fatalf("got %v, want view d's cluster", cds)
		}
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
		// Each stream on its own, so they don't unsubscribe at once.
		src, w := newSource(), &watched{}
		sotw := startSotw(t, newServer(t, src, xds.EnvoyConfig[string]{}, xds.Events[string]{}, w.option))
		sotw.request(xds.ClusterType, sotw.request(xds.ClusterType, nil)[0])
		update(src, cluster("bad\xff"))
		if err := <-sotw.ended; err == nil || w.cancelled != 1 {
			t.Errorf("the SotW stream ended with %v, unsubscribed %d times", err, w.cancelled)
		}

		src, w = newSource(), &watched{}
		delta := startDelta(t, newServer(t, src, xds.EnvoyConfig[string]{}, xds.Events[string]{}, w.option))
		delta.ack(delta.subscribe(xds.ClusterType)[0])
		update(src, cluster("bad\xff"))
		if err := <-delta.ended; err == nil || w.cancelled != 1 {
			t.Errorf("the delta stream ended with %v, unsubscribed %d times", err, w.cancelled)
		}
	})
}

func TestStreamErrorsAreWarnedAboutUnlessEnvoyWentAway(t *testing.T) {
	for name, c := range map[string]struct {
		end  func(e *deltaEnvoy)
		warn int
	}{
		"closed":    {func(e *deltaEnvoy) { close(e.stream.reqs) }, 0},
		"cancelled": {func(e *deltaEnvoy) { e.stream.errs <- status.Error(codes.Canceled, "gone") }, 0},
		"context":   {func(e *deltaEnvoy) { e.stream.errs <- context.Canceled }, 0},
		"broken":    {func(e *deltaEnvoy) { e.stream.errs <- status.Error(codes.Unavailable, "broken") }, 1},
	} {
		synctest.Test(t, func(t *testing.T) {
			l := &logs{}
			e := startDelta(t, newServer(t, newSource(), xds.EnvoyConfig[string]{}, xds.Events[string]{}, l.option))
			e.subscribe(xds.ClusterType)
			c.end(e)
			<-e.ended
			if got := l.count("Error streaming to an ADS client"); got != c.warn {
				t.Errorf("%s: warned %d times, want %d", name, got, c.warn)
			}
		})
	}
}
