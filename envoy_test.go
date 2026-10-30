package xds_test

import (
	"context"
	"io"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"testing/synctest"

	clusterv3 "github.com/envoyproxy/go-control-plane/envoy/config/cluster/v3"
	corev3 "github.com/envoyproxy/go-control-plane/envoy/config/core/v3"
	endpointv3 "github.com/envoyproxy/go-control-plane/envoy/config/endpoint/v3"
	listenerv3 "github.com/envoyproxy/go-control-plane/envoy/config/listener/v3"
	routev3 "github.com/envoyproxy/go-control-plane/envoy/config/route/v3"
	discoveryv3 "github.com/envoyproxy/go-control-plane/envoy/service/discovery/v3"
	statuspb "google.golang.org/genproto/googleapis/rpc/status"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/durationpb"

	xds "github.com/JonathanO/go-envoy-xds"
	"github.com/JonathanO/go-envoy-xds/snapshot"
)

// Tests play an Envoy on one stream, inside a synctest bubble: each request has been handled, and every response to
// it sent, by the time send returns. Time in the bubble is fake, so waits of 30 seconds take none.

type source = *snapshot.Source[string, string, string]

// newSource is one group, "g", in one view that every Envoy sees.
func newSource(rs ...proto.Message) source {
	src := snapshot.New[string, string, string](func(string) string { return "v" })
	update(src, rs...)
	src.Update(func(c *snapshot.Change[string, string]) { c.SetView("v", "g") })
	return src
}

// edit edits the group.
func edit(src source, f func(e *snapshot.Editor)) {
	src.Update(func(c *snapshot.Change[string, string]) { c.EditGroup("g", f) })
}

// update puts resources into the group.
func update(src source, rs ...proto.Message) {
	edit(src, func(e *snapshot.Editor) {
		for _, r := range rs {
			e.Put(r)
		}
	})
}

var allTypes = []xds.TypeURL{xds.ClusterType, xds.EndpointType, xds.ListenerType, xds.RouteType}

type serverConfig = xds.ServerConfig[*snapshot.Update, string]

func newServer(t *testing.T, src source, cfg xds.EnvoyConfig[string], events xds.Events[string], opts ...func(*serverConfig)) *xds.Server[*snapshot.Update, string] {
	t.Helper()
	c := serverConfig{
		Source:   src,
		Builders: snapshot.Builders[string](allTypes...),
		ConfigFor: func(context.Context, *corev3.Node) (xds.EnvoyConfig[string], error) {
			return cfg, nil
		},
		Events: events,
		Logger: slog.New(slog.DiscardHandler),
	}
	for _, o := range opts {
		o(&c)
	}
	srv, err := xds.NewServer(c)
	if err != nil {
		t.Fatal(err)
	}
	return srv
}

// logs is what a server logs, from debug up.
type logs struct{ strings.Builder }

func (l *logs) option(c *serverConfig) {
	c.Logger = slog.New(slog.NewTextHandler(l, &slog.HandlerOptions{Level: slog.LevelDebug}))
}

func (l *logs) count(msg string) int { return strings.Count(l.String(), msg) }

// metrics records every stream's metrics, in order.
type metrics struct{ calls []string }

func (m *metrics) option(c *serverConfig) { c.Metrics = func() xds.Metrics { return m } }
func (m *metrics) PushBegan()             { m.calls = append(m.calls, "began") }
func (m *metrics) PushCompleted()         { m.calls = append(m.calls, "completed") }
func (m *metrics) AwaitingAck()           { m.calls = append(m.calls, "awaiting") }
func (m *metrics) Acked()                 { m.calls = append(m.calls, "acked") }
func (m *metrics) Closed()                { m.calls = append(m.calls, "closed") }

// watched is a Source that counts the subscriptions to it that have been cancelled.
type watched struct {
	xds.Source[*snapshot.Update, string]
	cancelled int
}

func (w *watched) option(c *serverConfig) {
	w.Source = c.Source
	c.Source = w
}

func (w *watched) Subscribe(details string, deliver func(*snapshot.Update)) (*snapshot.Update, func()) {
	current, cancel := w.Source.Subscribe(details, deliver)
	return current, func() {
		w.cancelled++
		cancel()
	}
}

// serving has the server build every type, not only allTypes.
func serving(types ...xds.TypeURL) func(*serverConfig) {
	return func(c *serverConfig) { c.Builders = snapshot.Builders[string](types...) }
}

type fakeStream[Req, Resp any] struct {
	grpc.ServerStream
	ctx  context.Context
	reqs chan *Req  // closed when Envoy closes its side
	errs chan error // what Recv fails with
	mu   sync.Mutex
	sent []*Resp
	fail error // what sends return, if not nil
}

func (s *fakeStream[Req, Resp]) Context() context.Context { return s.ctx }

func (s *fakeStream[Req, Resp]) Recv() (*Req, error) {
	select {
	case r, ok := <-s.reqs:
		if !ok {
			return nil, io.EOF
		}
		return r, nil
	case err := <-s.errs:
		return nil, err
	case <-s.ctx.Done():
		return nil, s.ctx.Err()
	}
}

func (s *fakeStream[Req, Resp]) Send(r *Resp) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.fail != nil {
		return s.fail
	}
	s.sent = append(s.sent, r)
	return nil
}

// envoy is one fake Envoy's stream.
type envoy[Req, Resp any] struct {
	t      *testing.T
	stream *fakeStream[Req, Resp]
	read   int // responses already returned by sent
	ended  chan error
	end    context.CancelFunc
}

func start[Req, Resp any](t *testing.T, serve func(grpc.BidiStreamingServer[Req, Resp]) error) *envoy[Req, Resp] {
	ctx, cancel := context.WithCancel(t.Context())
	e := &envoy[Req, Resp]{t: t, stream: &fakeStream[Req, Resp]{ctx: ctx, reqs: make(chan *Req), errs: make(chan error)}, ended: make(chan error, 1), end: cancel}
	go func() { e.ended <- serve(e.stream) }()
	t.Cleanup(func() {
		cancel()
		synctest.Wait()
	})
	return e
}

// send sends a request, and returns the responses to it.
func (e *envoy[Req, Resp]) send(req *Req) []*Resp {
	e.t.Helper()
	select {
	case e.stream.reqs <- req:
	case err := <-e.ended:
		e.t.Fatalf("stream ended: %v", err)
	}
	synctest.Wait()
	return e.sent()
}

// sent returns the responses sent since the last call.
func (e *envoy[Req, Resp]) sent() []*Resp {
	synctest.Wait()
	e.stream.mu.Lock()
	defer e.stream.mu.Unlock()
	fresh := e.stream.sent[e.read:]
	e.read = len(e.stream.sent)
	return fresh
}

var node = &corev3.Node{Id: "envoy"}

type sotwEnvoy struct {
	*envoy[discoveryv3.DiscoveryRequest, discoveryv3.DiscoveryResponse]
}

func startSotw(t *testing.T, srv *xds.Server[*snapshot.Update, string]) *sotwEnvoy {
	return &sotwEnvoy{start(t, srv.StreamAggregatedResources)}
}

// request subscribes to names, answering prev if it's not nil.
func (e *sotwEnvoy) request(t xds.TypeURL, prev *discoveryv3.DiscoveryResponse, names ...string) []*discoveryv3.DiscoveryResponse {
	e.t.Helper()
	req := &discoveryv3.DiscoveryRequest{Node: node, TypeUrl: string(t), ResourceNames: names}
	if prev != nil {
		req.VersionInfo, req.ResponseNonce = prev.GetVersionInfo(), prev.GetNonce()
	}
	return e.send(req)
}

type deltaEnvoy struct {
	*envoy[discoveryv3.DeltaDiscoveryRequest, discoveryv3.DeltaDiscoveryResponse]
}

func startDelta(t *testing.T, srv *xds.Server[*snapshot.Update, string]) *deltaEnvoy {
	return &deltaEnvoy{start(t, srv.DeltaAggregatedResources)}
}

func (e *deltaEnvoy) subscribe(t xds.TypeURL, names ...string) []*discoveryv3.DeltaDiscoveryResponse {
	e.t.Helper()
	return e.send(&discoveryv3.DeltaDiscoveryRequest{Node: node, TypeUrl: string(t), ResourceNamesSubscribe: names})
}

func (e *deltaEnvoy) ack(r *discoveryv3.DeltaDiscoveryResponse) []*discoveryv3.DeltaDiscoveryResponse {
	e.t.Helper()
	return e.send(&discoveryv3.DeltaDiscoveryRequest{Node: node, TypeUrl: r.GetTypeUrl(), ResponseNonce: r.GetNonce()})
}

func (e *deltaEnvoy) nack(r *discoveryv3.DeltaDiscoveryResponse) []*discoveryv3.DeltaDiscoveryResponse {
	e.t.Helper()
	return e.send(&discoveryv3.DeltaDiscoveryRequest{Node: node, TypeUrl: r.GetTypeUrl(), ResponseNonce: r.GetNonce(),
		ErrorDetail: &statuspb.Status{Message: "no"}})
}

// reconnect subscribes as a reconnecting Envoy does, saying what it kept.
func (e *deltaEnvoy) reconnect(t xds.TypeURL, kept []string, names ...string) []*discoveryv3.DeltaDiscoveryResponse {
	e.t.Helper()
	versions := map[string]string{}
	for _, n := range kept {
		versions[n] = "1"
	}
	return e.send(&discoveryv3.DeltaDiscoveryRequest{Node: node, TypeUrl: string(t), ResourceNamesSubscribe: names,
		InitialResourceVersions: versions})
}

func cluster(name string) *clusterv3.Cluster {
	return &clusterv3.Cluster{Name: name, ConnectTimeout: durationpb.New(1e9)}
}

func cla(name, address string) *endpointv3.ClusterLoadAssignment {
	return &endpointv3.ClusterLoadAssignment{ClusterName: name, Endpoints: []*endpointv3.LocalityLbEndpoints{{
		LbEndpoints: []*endpointv3.LbEndpoint{{HostIdentifier: &endpointv3.LbEndpoint_Endpoint{Endpoint: &endpointv3.Endpoint{
			Address: &corev3.Address{Address: &corev3.Address_SocketAddress{SocketAddress: &corev3.SocketAddress{Address: address}}},
		}}}},
	}}}
}

func route(name, cluster string) *routev3.RouteConfiguration {
	return &routev3.RouteConfiguration{Name: name, VirtualHosts: []*routev3.VirtualHost{{Name: name, Domains: []string{"*"},
		Routes: []*routev3.Route{{Match: &routev3.RouteMatch{PathSpecifier: &routev3.RouteMatch_Prefix{Prefix: "/"}},
			Action: &routev3.Route_Route{Route: &routev3.RouteAction{ClusterSpecifier: &routev3.RouteAction_Cluster{Cluster: cluster}}}}}}}}
}

func listener(name string) *listenerv3.Listener { return &listenerv3.Listener{Name: name} }

// address returns the address of a cluster's first endpoint.
func address(t *testing.T, a *anypb.Any) string {
	t.Helper()
	var c endpointv3.ClusterLoadAssignment
	if err := a.UnmarshalTo(&c); err != nil {
		t.Fatal(err)
	}
	return c.GetEndpoints()[0].GetLbEndpoints()[0].GetEndpoint().GetAddress().GetSocketAddress().GetAddress()
}

// names returns the names a SotW response has.
func names(t *testing.T, r *discoveryv3.DiscoveryResponse) []string {
	t.Helper()
	var ns []string
	for _, a := range r.GetResources() {
		m, err := a.UnmarshalNew()
		if err != nil {
			t.Fatal(err)
		}
		switch m := m.(type) {
		case *endpointv3.ClusterLoadAssignment:
			ns = append(ns, m.GetClusterName())
		case interface{ GetName() string }:
			ns = append(ns, m.GetName())
		}
	}
	return ns
}

// deltaNames returns the names a delta response adds or changes.
func deltaNames(r *discoveryv3.DeltaDiscoveryResponse) []string {
	var ns []string
	for _, res := range r.GetResources() {
		ns = append(ns, res.GetName())
	}
	return ns
}

func types[R interface{ GetTypeUrl() string }](rs []R) []xds.TypeURL {
	var ts []xds.TypeURL
	for _, r := range rs {
		ts = append(ts, xds.TypeURL(r.GetTypeUrl()))
	}
	return ts
}
