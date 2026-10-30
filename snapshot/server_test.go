package snapshot_test

import (
	"context"
	"io"
	"log/slog"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"

	corev3 "github.com/envoyproxy/go-control-plane/envoy/config/core/v3"
	endpointv3 "github.com/envoyproxy/go-control-plane/envoy/config/endpoint/v3"
	listenerv3 "github.com/envoyproxy/go-control-plane/envoy/config/listener/v3"
	routev3 "github.com/envoyproxy/go-control-plane/envoy/config/route/v3"
	hcmv3 "github.com/envoyproxy/go-control-plane/envoy/extensions/filters/network/http_connection_manager/v3"
	discoveryv3 "github.com/envoyproxy/go-control-plane/envoy/service/discovery/v3"
	statuspb "google.golang.org/genproto/googleapis/rpc/status"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/anypb"

	xds "github.com/JonathanO/go-envoy-xds"
	"github.com/JonathanO/go-envoy-xds/snapshot"
)

// Tests here play an Envoy on one stream to a real Server, inside a synctest bubble, as the core's tests do.

// queued is a Source whose updates wait until the test delivers them, so several can pile up.
type queued struct {
	src
	waiting []func()
}

func (q *queued) Subscribe(details string, deliver func(*snapshot.Update)) (*snapshot.Update, func()) {
	return q.src.Subscribe(details, func(u *snapshot.Update) { q.waiting = append(q.waiting, func() { deliver(u) }) })
}

// deliver delivers the next update waiting, if there is one.
func (q *queued) deliver() bool {
	if len(q.waiting) == 0 {
		return false
	}
	f := q.waiting[0]
	q.waiting = q.waiting[1:]
	f()
	synctest.Wait()
	return true
}

func (q *queued) deliverAll() {
	for q.deliver() {
	}
}

// edit edits the group every Envoy sees.
func (q *queued) edit(f func(e *snapshot.Editor)) {
	q.Update(func(c *snapshot.Change[string, string]) { c.EditGroup("g", f) })
}

func newServer(t *testing.T, s xds.Source[*snapshot.Update, string], types ...xds.TypeURL) *xds.Server[*snapshot.Update, string] {
	t.Helper()
	srv, err := xds.NewServer(xds.ServerConfig[*snapshot.Update, string]{
		Source:   s,
		Builders: snapshot.Builders[string](types...),
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

// fake is the server's end of one Envoy's stream.
type fake[Req, Resp any] struct {
	grpc.ServerStream
	ctx  context.Context
	reqs chan *Req // closed when Envoy closes its side
	mu   sync.Mutex
	sent []*Resp
	read int           // responses already returned by received
	done chan struct{} // closed when the server ends the stream
	err  error         // what it ended it with
}

func (f *fake[Req, Resp]) Context() context.Context { return f.ctx }

func (f *fake[Req, Resp]) Recv() (*Req, error) {
	select {
	case r, ok := <-f.reqs:
		if !ok {
			return nil, io.EOF
		}
		return r, nil
	case <-f.ctx.Done():
		return nil, f.ctx.Err()
	}
}

func (f *fake[Req, Resp]) Send(r *Resp) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.sent = append(f.sent, r)
	return nil
}

// connect starts a stream in the test's bubble, which ends with the test.
func connect[Req, Resp any](t *testing.T, serve func(grpc.BidiStreamingServer[Req, Resp]) error) *fake[Req, Resp] {
	ctx, cancel := context.WithCancel(t.Context())
	f := &fake[Req, Resp]{ctx: ctx, reqs: make(chan *Req), done: make(chan struct{})}
	go func() {
		defer close(f.done)
		f.err = serve(f)
	}()
	t.Cleanup(func() {
		cancel()
		synctest.Wait()
	})
	return f
}

// check fails the test if the server has ended the stream. Until the test ends, only Envoy may.
func (f *fake[Req, Resp]) check(t *testing.T) {
	t.Helper()
	select {
	case <-f.done:
		t.Fatalf("the server ended the stream: %v", f.err)
	default:
	}
}

// send sends a request, and returns the responses to it.
func (f *fake[Req, Resp]) send(r *Req) []*Resp {
	f.reqs <- r
	return f.received()
}

// received returns the responses sent since the last call.
func (f *fake[Req, Resp]) received() []*Resp {
	synctest.Wait()
	f.mu.Lock()
	defer f.mu.Unlock()
	rs := f.sent[f.read:]
	f.read = len(f.sent)
	return rs
}

var node = &corev3.Node{Id: "envoy"}

// sotwStream is a SotW stream from one Envoy to a Source of one group, which every Envoy sees. Updates arrive when
// the test says, and after each request.
type sotwStream struct {
	t   *testing.T
	src *queued
	*fake[discoveryv3.DiscoveryRequest, discoveryv3.DiscoveryResponse]
	responses []*discoveryv3.DiscoveryResponse
}

func newSotwStream(t *testing.T, types ...xds.TypeURL) *sotwStream {
	q := &queued{src: snapshot.New[string, string, string](everyone)}
	q.Update(func(c *snapshot.Change[string, string]) { c.SetView("v", "g") })
	return &sotwStream{t: t, src: q, fake: connect(t, newServer(t, q, types...).StreamAggregatedResources)}
}

// request subscribes to names, answering prev if it's not nil, and returns the answer.
func (s *sotwStream) request(typ xds.TypeURL, prev *discoveryv3.DiscoveryResponse, names ...string) *discoveryv3.DiscoveryResponse {
	s.t.Helper()
	req := &discoveryv3.DiscoveryRequest{TypeUrl: string(typ), ResourceNames: names}
	if prev != nil {
		req.VersionInfo, req.ResponseNonce = prev.GetVersionInfo(), prev.GetNonce()
	}
	before := len(s.responses)
	s.send(req)
	if len(s.responses) == before {
		s.t.Fatalf("no answer to %s", typ)
	}
	return s.last()
}

// ack ACKs a response. SotW requests list every name subscribed to, ACKs included.
func (s *sotwStream) ack(r *discoveryv3.DiscoveryResponse, names ...string) {
	s.send(&discoveryv3.DiscoveryRequest{TypeUrl: r.GetTypeUrl(), VersionInfo: r.GetVersionInfo(), ResponseNonce: r.GetNonce(), ResourceNames: names})
}

func (s *sotwStream) send(req *discoveryv3.DiscoveryRequest) {
	req.Node = node
	s.responses = append(s.responses, s.fake.send(req)...)
	s.deliver()
}

// deliver delivers the Source's updates to the stream.
func (s *sotwStream) deliver() {
	s.src.deliverAll()
	s.responses = append(s.responses, s.received()...)
	s.check(s.t)
}

func (s *sotwStream) last() *discoveryv3.DiscoveryResponse { return s.responses[len(s.responses)-1] }

// previous returns the last response of a type, or one back before it.
func (s *sotwStream) previous(typ xds.TypeURL, back int) *discoveryv3.DiscoveryResponse {
	var of []*discoveryv3.DiscoveryResponse
	for _, r := range s.responses {
		if r.GetTypeUrl() == string(typ) {
			of = append(of, r)
		}
	}
	return of[len(of)-1-back]
}

func cla(name string, ips ...string) *endpointv3.ClusterLoadAssignment {
	var lbs []*endpointv3.LbEndpoint
	for _, ip := range ips {
		lbs = append(lbs, &endpointv3.LbEndpoint{HostIdentifier: &endpointv3.LbEndpoint_Endpoint{Endpoint: &endpointv3.Endpoint{
			Address: &corev3.Address{Address: &corev3.Address_SocketAddress{SocketAddress: &corev3.SocketAddress{Address: ip,
				PortSpecifier: &corev3.SocketAddress_PortValue{PortValue: 80}}}},
		}}})
	}
	return &endpointv3.ClusterLoadAssignment{ClusterName: name, Endpoints: []*endpointv3.LocalityLbEndpoints{{LbEndpoints: lbs}}}
}

// routes is a route config with one virtual host, routing "/" + cluster to each cluster.
func routes(name string, clusters ...string) *routev3.RouteConfiguration {
	vh := &routev3.VirtualHost{Name: "default", Domains: []string{"*"}}
	for _, c := range clusters {
		vh.Routes = append(vh.Routes, &routev3.Route{Match: &routev3.RouteMatch{PathSpecifier: &routev3.RouteMatch_Prefix{Prefix: "/" + c}},
			Action: &routev3.Route_Route{Route: &routev3.RouteAction{ClusterSpecifier: &routev3.RouteAction_Cluster{Cluster: c}}}})
	}
	return &routev3.RouteConfiguration{Name: name, VirtualHosts: []*routev3.VirtualHost{vh}}
}

// listener is a listener whose HTTP connection manager gets its routes over RDS.
func listener(name, routeConfig string) *listenerv3.Listener {
	hcm, err := anypb.New(&hcmv3.HttpConnectionManager{StatPrefix: name, RouteSpecifier: &hcmv3.HttpConnectionManager_Rds{Rds: &hcmv3.Rds{
		RouteConfigName: routeConfig,
		ConfigSource:    &corev3.ConfigSource{ResourceApiVersion: corev3.ApiVersion_V3, ConfigSourceSpecifier: &corev3.ConfigSource_Ads{}},
	}}})
	if err != nil {
		panic(err)
	}
	return &listenerv3.Listener{Name: name, FilterChains: []*listenerv3.FilterChain{{Filters: []*listenerv3.Filter{{
		Name: "envoy.filters.network.http_connection_manager", ConfigType: &listenerv3.Filter_TypedConfig{TypedConfig: hcm},
	}}}}}
}

// endpoints returns each cluster and its addresses in an EDS response.
func endpoints(t *testing.T, r *discoveryv3.DiscoveryResponse) []string {
	t.Helper()
	var got []string
	for _, a := range r.GetResources() {
		var c endpointv3.ClusterLoadAssignment
		if err := a.UnmarshalTo(&c); err != nil {
			t.Fatal(err)
		}
		e := c.GetClusterName()
		for _, lb := range c.GetEndpoints()[0].GetLbEndpoints() {
			e += " " + lb.GetEndpoint().GetAddress().GetSocketAddress().GetAddress()
		}
		got = append(got, e)
	}
	return got
}

func TestSotwSubscribingAfterANackSendsNoMoreOfTheRejectedChange(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		s := newSotwStream(t, xds.ClusterType, xds.EndpointType)
		s.src.edit(func(e *snapshot.Editor) { e.Put(cluster("a", 1)); e.Put(cla("a", "10.0.0.1")) })
		cds := s.request(xds.ClusterType, nil)
		s.ack(cds)
		s.ack(s.request(xds.EndpointType, nil, "a"), "a")

		// Clusters go first, so the new endpoints for a wait for Envoy to ACK them. It NACKs instead.
		s.src.edit(func(e *snapshot.Editor) {
			e.Put(cluster("b", 1))
			e.Put(cla("a", "10.0.0.2"))
			e.Put(cla("b", "10.0.0.3"))
		})
		s.deliver()
		rejected := s.last()
		if rejected.GetTypeUrl() != string(xds.ClusterType) {
			t.Fatalf("got %s, want the clusters first", rejected.GetTypeUrl())
		}
		s.send(&discoveryv3.DiscoveryRequest{TypeUrl: string(xds.ClusterType), VersionInfo: cds.GetVersionInfo(),
			ResponseNonce: rejected.GetNonce(), ErrorDetail: &statuspb.Status{Message: "bad cluster"}})

		// Envoy took cluster b anyway, so it asks for b's endpoints.
		eds := s.request(xds.EndpointType, s.previous(xds.EndpointType, 0), "a", "b")
		if got := endpoints(t, eds); !slices.Equal(got, []string{"a 10.0.0.1", "b 10.0.0.3"}) {
			t.Fatalf("got %v, want a's endpoints as Envoy has them, and b's, newly asked for", got)
		}
	})
}

func TestSotwSubscribingAfterANackOfTheSameTypeRepeatsWhatWasRejected(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		s := newSotwStream(t, xds.ClusterType, xds.EndpointType)
		s.src.edit(func(e *snapshot.Editor) { e.Put(cluster("a", 1)); e.Put(cla("a", "10.0.0.1")) })
		s.ack(s.request(xds.ClusterType, nil))
		s.ack(s.request(xds.EndpointType, nil, "a"), "a")

		s.src.edit(func(e *snapshot.Editor) {
			e.Put(cluster("b", 1))
			e.Put(cla("a", "10.0.0.2"))
			e.Put(cla("b", "10.0.0.3"))
		})
		s.deliver()
		s.ack(s.last())
		rejected := s.last()
		if got := endpoints(t, rejected); !slices.Equal(got, []string{"a 10.0.0.2"}) {
			t.Fatalf("got %v, want a's new endpoints", got)
		}

		// Envoy NACKs the endpoints, and in the same request asks for b's, as it took cluster b.
		before := len(s.responses)
		s.send(&discoveryv3.DiscoveryRequest{TypeUrl: string(xds.EndpointType), VersionInfo: s.previous(xds.EndpointType, 1).GetVersionInfo(),
			ResponseNonce: rejected.GetNonce(), ResourceNames: []string{"a", "b"}, ErrorDetail: &statuspb.Status{Message: "bad endpoints"}})

		// Envoy may have taken part of what it rejected, so the only answer that leaves it as it is, is the same one.
		// Rolling a back would also roll back anything else in that answer that Envoy took.
		if len(s.responses) != before+1 {
			t.Fatalf("got %d answers, want one", len(s.responses)-before)
		}
		if got := endpoints(t, s.last()); !slices.Equal(got, []string{"a 10.0.0.2", "b 10.0.0.3"}) {
			t.Fatalf("got %v, want what Envoy rejected again, as SotW sends the whole set, and b, newly asked for", got)
		}
	})
}

func TestSotwUnsubscribingDuringAChangeStillGetsTheChangeInTurn(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		s := newSotwStream(t, xds.ClusterType, xds.EndpointType)
		s.src.edit(func(e *snapshot.Editor) {
			e.Put(cluster("a", 1))
			e.Put(cluster("b", 1))
			e.Put(cla("a", "10.0.0.1"))
			e.Put(cla("b", "10.0.0.2"))
		})
		s.ack(s.request(xds.ClusterType, nil))
		eds := s.request(xds.EndpointType, nil, "a", "b")
		s.ack(eds, "a", "b")

		// Endpoints wait for the new cluster to be ACKed. Envoy unsubscribes from b's meanwhile.
		s.src.edit(func(e *snapshot.Editor) { e.Put(cluster("c", 1)); e.Put(cla("a", "10.0.0.3")) })
		s.deliver()
		cds := s.last()
		if cds.GetTypeUrl() != string(xds.ClusterType) {
			t.Fatalf("got %s, want the clusters first", cds.GetTypeUrl())
		}
		s.ack(eds, "a")
		if s.last() != cds {
			t.Fatalf("got %v, want no answer to the unsubscribe", s.last())
		}

		s.ack(cds)
		if got := endpoints(t, s.last()); !slices.Equal(got, []string{"a 10.0.0.3"}) {
			t.Fatalf("got %v, want the change, once its clusters are ACKed, without b", got)
		}
	})
}

func TestALaterPhaseWaitsForTheEarlierOnesAckEvenWhenBothQueue(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		s := newSotwStream(t, xds.ClusterType, xds.EndpointType, xds.RouteType)
		s.src.edit(func(e *snapshot.Editor) { e.Put(cluster("a", 1)); e.Put(cla("a", "10.0.0.1")); e.Put(routes("r", "a")) })
		s.ack(s.request(xds.ClusterType, nil))
		s.ack(s.request(xds.EndpointType, nil, "a"), "a")
		s.ack(s.request(xds.RouteType, nil, "r"), "r")

		// Envoy is busy with new endpoints when the phased change arrives, so both phases wait for it.
		s.src.edit(func(e *snapshot.Editor) { e.Put(cla("a", "10.0.0.2")) })
		s.deliver()
		busy := s.last()
		s.src.Update(func(c *snapshot.Change[string, string]) {
			c.EditGroup("g", func(e *snapshot.Editor) { e.Put(routes("r")) }) // stop routing to a first
			c.Then()
			c.EditGroup("g", func(e *snapshot.Editor) { e.Put(cluster("a", 2)) })
		})
		s.deliver()
		s.ack(busy, "a")

		// Without phases the cluster would go first, as make before break sends clusters before routes.
		rds := s.last()
		if rds.GetTypeUrl() != string(xds.RouteType) {
			t.Fatalf("got %s, want the first phase's routes", rds.GetTypeUrl())
		}
		before := len(s.responses)
		s.ack(rds, "r")
		if len(s.responses) != before+1 || s.last().GetTypeUrl() != string(xds.ClusterType) {
			t.Fatalf("got %d more, the last %s, want the second phase once the first is ACKed", len(s.responses)-before, s.last().GetTypeUrl())
		}
	})
}

// checked is a stream that reports what gRPC forbids of a server: calls to Send that overlap, or follow the end.
type checked struct {
	fake[discoveryv3.DeltaDiscoveryRequest, discoveryv3.DeltaDiscoveryResponse]
	inSend, ended atomic.Bool
	problems      atomic.Int32
}

func (c *checked) Send(r *discoveryv3.DeltaDiscoveryResponse) error {
	if !c.inSend.CompareAndSwap(false, true) || c.ended.Load() {
		c.problems.Add(1)
	}
	defer c.inSend.Store(false)
	return c.fake.Send(r)
}

func TestAStreamEndingWhileChangesArriveEndsCleanly(t *testing.T) {
	for range 100 {
		s := snapshot.New[string, string, string](everyone)
		s.Update(func(c *snapshot.Change[string, string]) { c.SetView("v", "g") })
		edit := func(f func(e *snapshot.Editor)) {
			s.Update(func(c *snapshot.Change[string, string]) { c.EditGroup("g", f) })
		}
		srv := newServer(t, s, xds.ClusterType)
		stream := &checked{fake: fake[discoveryv3.DeltaDiscoveryRequest, discoveryv3.DeltaDiscoveryResponse]{ctx: t.Context(),
			reqs: make(chan *discoveryv3.DeltaDiscoveryRequest)}}
		var wg sync.WaitGroup
		// gRPC delivers one stream's requests one at a time; changes arrive from elsewhere.
		wg.Go(func() {
			stream.reqs <- &discoveryv3.DeltaDiscoveryRequest{Node: node, TypeUrl: string(xds.ClusterType)}
			stream.reqs <- &discoveryv3.DeltaDiscoveryRequest{TypeUrl: string(xds.ClusterType), ResourceNamesSubscribe: []string{"a"}}
			close(stream.reqs)
		})
		wg.Go(func() {
			edit(func(e *snapshot.Editor) { e.Put(cluster("a", 1)) })
			edit(func(e *snapshot.Editor) { e.Put(cluster("b", 1)) })
			edit(func(e *snapshot.Editor) { e.Remove(xds.ClusterType, "a") })
		})
		if err := srv.DeltaAggregatedResources(stream); err != nil {
			t.Fatal(err)
		}
		stream.ended.Store(true)
		wg.Wait()
		edit(func(e *snapshot.Editor) { e.Put(cluster("c", 1)) }) // nothing may follow the end
		if n := stream.problems.Load(); n != 0 {
			t.Fatalf("%d sends overlapped or followed the end", n)
		}
	}
}
