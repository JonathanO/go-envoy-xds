package xds_test

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"slices"
	"strings"
	"testing"
	"testing/synctest"

	clusterv3 "github.com/envoyproxy/go-control-plane/envoy/config/cluster/v3"
	corev3 "github.com/envoyproxy/go-control-plane/envoy/config/core/v3"
	endpointv3 "github.com/envoyproxy/go-control-plane/envoy/config/endpoint/v3"
	listenerv3 "github.com/envoyproxy/go-control-plane/envoy/config/listener/v3"
	routev3 "github.com/envoyproxy/go-control-plane/envoy/config/route/v3"
	tlsv3 "github.com/envoyproxy/go-control-plane/envoy/extensions/transport_sockets/tls/v3"
	discoveryv3 "github.com/envoyproxy/go-control-plane/envoy/service/discovery/v3"
	runtimev3 "github.com/envoyproxy/go-control-plane/envoy/service/runtime/v3"
	statuspb "google.golang.org/genproto/googleapis/rpc/status"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"

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
		denied, l := errors.New("denied"), &logs{}
		e := startSotw(t, newServer(t, newSource(), xds.EnvoyConfig[string]{}, xds.Events[string]{}, l.option, func(c *serverConfig) {
			c.ConfigFor = func(context.Context, *corev3.Node) (xds.EnvoyConfig[string], error) {
				return xds.EnvoyConfig[string]{}, denied
			}
		}))
		e.stream.reqs <- nil
		if err := <-e.ended; !errors.Is(err, denied) {
			t.Fatalf("got %v", err)
		}
		if got := l.count(`level=WARN msg="Error streaming to an ADS client" error=denied`); got != 1 {
			t.Fatalf("logged %q", l.String())
		}
	})
}

func TestAResyncIsLogged(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var client *xds.Client[string]
		l := &logs{}
		e := startSotw(t, newServer(t, newSource(), xds.EnvoyConfig[string]{}, xds.Events[string]{
			Connected: func(c *xds.Client[string]) { client = c },
		}, l.option))
		e.request(xds.ClusterType, nil)
		client.Resync()
		<-e.ended
		if got := l.count(`level=INFO msg="Closing stream so it resyncs" node=envoy`); got != 1 {
			t.Fatalf("logged %q", l.String())
		}
	})
}

// Asking a missing builder would panic, which now only ends the stream, so check it stays up.
func TestATypeNobodyServesSurvivesAChange(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		src := newSource(cluster("a"))
		e := startSotw(t, newServer(t, src, xds.EnvoyConfig[string]{}, xds.Events[string]{}, serving(xds.ClusterType)))
		e.request(xds.EndpointType, e.request(xds.EndpointType, nil, "a")[0], "a")
		e.request(xds.ClusterType, e.request(xds.ClusterType, nil)[0])
		update(src, cluster("b"))
		e.request(xds.ClusterType, e.sent()[0]) // on to the remove round
		select {
		case err := <-e.ended:
			t.Fatalf("the stream ended with %v", err)
		default:
		}
	})
}

// panicking panics when building a change for the Envoy whose details are "bad".
type panicking struct {
	xds.ConfigBuilder[*snapshot.Update, string]
}

func (p panicking) AddOrder(u *snapshot.Update, subscribed func(string) bool, d string) xds.Response {
	if d == "bad" {
		panic("broken builder")
	}
	return p.ConfigBuilder.AddOrder(u, subscribed, d)
}

func TestAPanickingBuilderEndsOnlyItsStream(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		src, w, l := newSource(cluster("a")), &watched{}, &logs{}
		var disconnected []string
		srv := newServer(t, src, xds.EnvoyConfig[string]{}, xds.Events[string]{
			Disconnected: func(c *xds.Client[string]) { disconnected = append(disconnected, c.Node.GetId()) },
		}, w.option, l.option, func(c *serverConfig) {
			c.Builders = []xds.ConfigBuilder[*snapshot.Update, string]{panicking{snapshot.Builders[string](xds.ClusterType)[0]}}
			c.ConfigFor = func(_ context.Context, n *corev3.Node) (xds.EnvoyConfig[string], error) {
				return xds.EnvoyConfig[string]{Details: n.GetId()}, nil
			}
		})
		bad := startSotw(t, srv)
		badNode := &corev3.Node{Id: "bad"}
		cds := bad.send(&discoveryv3.DiscoveryRequest{Node: badNode, TypeUrl: string(xds.ClusterType)})[0]
		bad.send(&discoveryv3.DiscoveryRequest{Node: badNode, TypeUrl: string(xds.ClusterType), VersionInfo: cds.GetVersionInfo(), ResponseNonce: cds.GetNonce()})
		good := startSotw(t, srv)
		good.request(xds.ClusterType, good.request(xds.ClusterType, nil)[0])

		update(src, cluster("b"))
		if err := <-bad.ended; status.Code(err) != codes.Internal {
			t.Fatalf("the bad stream ended with %v", err)
		}
		if w.cancelled != 1 || !slices.Equal(disconnected, []string{"bad"}) {
			t.Fatalf("unsubscribed %d times, disconnected %v", w.cancelled, disconnected)
		}
		if got := l.count(`level=ERROR msg="Panicked serving an Envoy, so closing its stream" node=bad panic="broken builder"`); got != 1 {
			t.Fatalf("logged %q", l.String())
		}
		if sent := good.sent(); len(sent) != 1 || !slices.Equal(names(t, sent[0]), []string{"a", "b"}) {
			t.Fatalf("got %v on the other stream, want the change", sent)
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

// Whatever an Envoy sends, in whatever order, and whatever the Source does meanwhile, a stream answers only what was
// asked, fails only for protocol violations, and always cleans up when it ends. Runs the real stack, both protocols.
func FuzzStreamsStayConsistent(f *testing.F) {
	r := rand.New(rand.NewPCG(1, 2))
	for range 1000 {
		data := make([]byte, r.IntN(256))
		for i := range data {
			data[i] = byte(r.UintN(256))
		}
		f.Add(data)
	}
	f.Fuzz(func(t *testing.T, data []byte) {
		synctest.Test(t, func(t *testing.T) {
			d := draws(data)
			if d.next(2) == 0 {
				fuzzStream(t, &d, sotwProtocol)
			} else {
				fuzzStream(t, &d, deltaProtocol)
			}
		})
	})
}

func fuzzStream[Req, Resp any](t *testing.T, d *draws, p protocol[Req, Resp]) {
	src := &broadcast{subscribers: map[int]func(int){}}
	var builders []xds.ConfigBuilder[int, string]
	for typ, build := range messages {
		builders = append(builders, named{typ, build})
	}
	var client *xds.Client[string]
	connected, disconnected, nacks, nacksSent := 0, 0, 0, 0
	srv, err := xds.NewServer(xds.ServerConfig[int, string]{
		Source:   src,
		Builders: builders,
		ConfigFor: func(context.Context, *corev3.Node) (xds.EnvoyConfig[string], error) {
			return xds.EnvoyConfig[string]{}, nil
		},
		Events: xds.Events[string]{
			Connected:    func(c *xds.Client[string]) { connected++; client = c },
			Disconnected: func(*xds.Client[string]) { disconnected++ },
			Nacked:       func(*xds.Client[string], xds.TypeURL, *statuspb.Status) { nacks++ },
		},
		Logger: slog.New(slog.DiscardHandler),
	})
	if err != nil {
		t.Fatal(err)
	}
	e := start(t, p.serve(srv))
	asked := map[string]bool{}
	var nonces []string
	live, envoyEnded := true, false
	for len(*d) > 0 {
		switch action := d.next(11); {
		case action < 6:
			if !live {
				continue
			}
			typ := typeURLs[d.next(len(typeURLs))]
			asked[typ] = true
			n := &corev3.Node{Id: []string{"", "a", "envoy"}[d.next(3)]}
			nonce := []string{"", "bogus"}[d.next(2)]
			if len(nonces) > 0 && d.next(2) == 1 {
				nonce = nonces[d.next(len(nonces))]
			}
			var detail *statuspb.Status
			if d.next(10) == 0 {
				detail = &statuspb.Status{Code: 3, Message: "rejected"}
				if messages[xds.TypeURL(typ)] != nil {
					nacksSent++
				}
			}
			e.stream.reqs <- p.request(d, n, typ, nonce, detail) // its responses are checked below
		case action < 8:
			src.change()
		case action < 9:
			if !live || client == nil {
				continue
			}
			client.Resync()
		case action < 10:
			if !live {
				continue
			}
			envoyEnded = true
			e.end()
		default:
			if !live {
				continue
			}
			envoyEnded = true
			close(e.stream.reqs)
		}

		for _, r := range e.sent() {
			if !live {
				t.Fatal("a response after the stream ended")
			}
			typ, nonce, resources := p.response(r)
			if !asked[typ] || messages[xds.TypeURL(typ)] == nil {
				t.Fatalf("answered %q, which wasn't asked for or isn't known", typ)
			}
			for _, a := range resources {
				if a.GetTypeUrl() != typ {
					t.Fatalf("a %s in a response of %s", a.GetTypeUrl(), typ)
				}
			}
			// Go's SotW lists resources by name, where Java's keeps them in the order they came.
			if r, ok := any(r).(*discoveryv3.DiscoveryResponse); ok && !slices.IsSorted(names(t, r)) {
				t.Fatalf("resources out of order: %v", names(t, r))
			}
			nonces = append(nonces, nonce)
		}
		select {
		case err := <-e.ended:
			live = false
			violation := status.Code(err) == codes.InvalidArgument && strings.Contains(err.Error(), "subscribed to and unsubscribed from")
			if !envoyEnded && err != nil && !violation {
				t.Fatalf("the stream ended with %v", err)
			}
		default:
		}
		if !live && (len(src.subscribers) != 0 || disconnected != connected) {
			t.Fatalf("ended with %d subscriptions to the source, %d connects and %d disconnects", len(src.subscribers), connected, disconnected)
		}
		if connected > 1 {
			t.Fatalf("connected %d times", connected)
		}
		if nacks != nacksSent {
			t.Fatalf("told of %d NACKs of %d", nacks, nacksSent)
		}
	}
}

// draws reads a fuzzer's bytes as choices: each byte picks one of n, and 0 once they run out.
type draws []byte

func (d *draws) next(n int) int {
	if len(*d) == 0 {
		return 0
	}
	b := (*d)[0]
	*d = (*d)[1:]
	return int(b) % n
}

// subset picks from a, b and the wildcard, by the bits of a choice.
func subset(bits int) []string {
	var ns []string
	for i, n := range []string{"a", "b", "*"} {
		if bits&(1<<i) != 0 {
			ns = append(ns, n)
		}
	}
	return ns
}

// A protocol is how the fuzzer talks SotW or delta.
type protocol[Req, Resp any] struct {
	serve    func(*xds.Server[int, string]) func(grpc.BidiStreamingServer[Req, Resp]) error
	request  func(d *draws, n *corev3.Node, typ, nonce string, detail *statuspb.Status) *Req
	response func(*Resp) (typ, nonce string, resources []*anypb.Any)
}

var sotwProtocol = protocol[discoveryv3.DiscoveryRequest, discoveryv3.DiscoveryResponse]{
	serve: func(s *xds.Server[int, string]) func(grpc.BidiStreamingServer[discoveryv3.DiscoveryRequest, discoveryv3.DiscoveryResponse]) error {
		return s.StreamAggregatedResources
	},
	request: func(d *draws, n *corev3.Node, typ, nonce string, detail *statuspb.Status) *discoveryv3.DiscoveryRequest {
		return &discoveryv3.DiscoveryRequest{Node: n, TypeUrl: typ, ResponseNonce: nonce, ErrorDetail: detail,
			VersionInfo: []string{"", "1", "2", "not-a-number"}[d.next(4)], ResourceNames: subset(d.next(8))}
	},
	response: func(r *discoveryv3.DiscoveryResponse) (string, string, []*anypb.Any) {
		return r.GetTypeUrl(), r.GetNonce(), r.GetResources()
	},
}

var deltaProtocol = protocol[discoveryv3.DeltaDiscoveryRequest, discoveryv3.DeltaDiscoveryResponse]{
	serve: func(s *xds.Server[int, string]) func(grpc.BidiStreamingServer[discoveryv3.DeltaDiscoveryRequest, discoveryv3.DeltaDiscoveryResponse]) error {
		return s.DeltaAggregatedResources
	},
	request: func(d *draws, n *corev3.Node, typ, nonce string, detail *statuspb.Status) *discoveryv3.DeltaDiscoveryRequest {
		kept := map[string]string{}
		for _, name := range subset(d.next(4)) { // a and b
			kept[name] = "v"
		}
		return &discoveryv3.DeltaDiscoveryRequest{Node: n, TypeUrl: typ, ResponseNonce: nonce, ErrorDetail: detail,
			ResourceNamesSubscribe: subset(d.next(8)), ResourceNamesUnsubscribe: subset(d.next(8)), InitialResourceVersions: kept}
	},
	response: func(r *discoveryv3.DeltaDiscoveryResponse) (string, string, []*anypb.Any) {
		var as []*anypb.Any
		for _, res := range r.GetResources() {
			as = append(as, res.GetResource())
		}
		return r.GetTypeUrl(), r.GetNonce(), as
	},
}

// messages builds a resource of each type, by name and update.
var messages = map[xds.TypeURL]func(name string, update int) proto.Message{
	xds.SecretType:  func(n string, _ int) proto.Message { return &tlsv3.Secret{Name: n} },
	xds.RuntimeType: func(n string, _ int) proto.Message { return &runtimev3.Runtime{Name: n} },
	xds.ClusterType: func(n string, u int) proto.Message {
		return &clusterv3.Cluster{Name: n, AltStatName: fmt.Sprint("v", u)}
	},
	xds.EndpointType:    func(n string, _ int) proto.Message { return &endpointv3.ClusterLoadAssignment{ClusterName: n} },
	xds.ListenerType:    func(n string, _ int) proto.Message { return &listenerv3.Listener{Name: n} },
	xds.RouteType:       func(n string, _ int) proto.Message { return &routev3.RouteConfiguration{Name: n} },
	xds.ScopedRouteType: func(n string, _ int) proto.Message { return &routev3.ScopedRouteConfiguration{Name: n} },
	xds.VirtualHostType: func(n string, _ int) proto.Message { return &routev3.VirtualHost{Name: n} },
}

// typeURLs are what the fuzzer asks for: every type, one nobody serves, and none.
var typeURLs = []string{string(xds.SecretType), string(xds.RuntimeType), string(xds.ClusterType), string(xds.EndpointType),
	string(xds.ListenerType), string(xds.RouteType), string(xds.ScopedRouteType), string(xds.VirtualHostType),
	"type.googleapis.com/envoy.config.core.v3.TypedExtensionConfig", ""}

// named serves names a and b of one type, changing with the update.
type named struct {
	typ   xds.TypeURL
	build func(name string, update int) proto.Message
}

func (b named) Type() xds.TypeURL { return b.typ }

func (b named) resources(u int, subscribed func(string) bool) []xds.Resource {
	var rs []xds.Resource
	for _, n := range []string{"a", "b"} {
		if subscribed(n) {
			rs = append(rs, xds.Resource{Name: n, Message: b.build(n, u)})
		}
	}
	return rs
}

func (b named) AddOrder(u int, subscribed func(string) bool, _ string) xds.Response {
	return xds.Response{Updates: b.resources(u, subscribed)}
}
func (b named) RemoveOrder(int, func(string) bool, string) xds.Response { return xds.Response{} }
func (b named) ResourcesAddOrder(u int, subscribed func(string) bool, _ string) []xds.Resource {
	return b.resources(u, subscribed)
}
func (b named) ResourcesRemoveOrder(u int, subscribed func(string) bool, _ string) []xds.Resource {
	return b.resources(u, subscribed)
}

// broadcast is a Source of ints, each update the next, to every subscriber.
type broadcast struct {
	current     int
	subscribers map[int]func(int)
	ids         int
}

func (b *broadcast) Subscribe(_ string, deliver func(int)) (int, func()) {
	b.ids++
	id := b.ids
	b.subscribers[id] = deliver
	return b.current, func() { delete(b.subscribers, id) }
}

func (b *broadcast) change() {
	b.current++
	for _, deliver := range b.subscribers {
		deliver(b.current)
	}
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
		"invalid": {func(e *deltaEnvoy) {
			e.stream.reqs <- &discoveryv3.DeltaDiscoveryRequest{Node: node, TypeUrl: string(xds.ClusterType),
				ResourceNamesSubscribe: []string{"a"}, ResourceNamesUnsubscribe: []string{"a"}}
		}, 1},
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
