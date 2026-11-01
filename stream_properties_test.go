package xds_test

import (
	"context"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"slices"
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
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"

	xds "github.com/JonathanO/go-envoy-xds"
)

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
	heard := map[string]*subscription{} // by type asked for
	var nonces []string
	live, envoyEnded := true, false
	for len(*d) > 0 {
		switch action := d.next(11); {
		case action < 6:
			if !live {
				continue
			}
			typ := typeURLs[d.next(len(typeURLs))]
			if heard[typ] == nil {
				heard[typ] = &subscription{names: map[string]bool{}}
			}
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
			req := p.request(d, n, typ, nonce, detail)
			heard[typ].hear(req, xds.TypeURL(typ).Wildcard())
			e.stream.reqs <- req // its responses are checked below
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
			h := heard[typ]
			if h == nil || messages[xds.TypeURL(typ)] == nil {
				t.Fatalf("answered %q, which wasn't asked for or isn't known", typ)
			}
			for _, a := range resources {
				if a.GetTypeUrl() != typ {
					t.Fatalf("a %s in a response of %s", a.GetTypeUrl(), typ)
				}
			}
			var sent []string
			switch r := any(r).(type) {
			case *discoveryv3.DiscoveryResponse:
				// SotW lists resources by name.
				if sent = names(t, r); !slices.IsSorted(sent) {
					t.Fatalf("resources out of order: %v", sent)
				}
			case *discoveryv3.DeltaDiscoveryResponse:
				sent = deltaNames(r)
			}
			for _, name := range sent {
				if !mayWait[xds.TypeURL(typ)] && !h.all && !h.names[name] {
					t.Fatalf("sent %s %s, which Envoy isn't subscribed to", typ, name)
				}
			}
			h.nonce = nonce
			nonces = append(nonces, nonce)
		}
		select {
		case err := <-e.ended:
			live = false
			if !envoyEnded && err != nil {
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

// mayWait are the types whose requests may wait for clusters, and be answered after later ones.
var mayWait = map[xds.TypeURL]bool{xds.ListenerType: true, xds.RouteType: true, xds.ScopedRouteType: true, xds.VirtualHostType: true}

// subscription is what the server has heard Envoy subscribe to, of one type.
type subscription struct {
	all    bool // to everything
	before bool // for SotW, named something before; for delta, asked before
	names  map[string]bool
	nonce  string // of the last response: SotW ignores a request answering any other
}

func (s *subscription) hear(req any, wild bool) {
	switch req := req.(type) {
	case *discoveryv3.DiscoveryRequest:
		if s.nonce != "" && req.GetResponseNonce() != s.nonce {
			return
		}
		ns := req.GetResourceNames()
		s.all = wild && (slices.Contains(ns, "*") || (len(ns) == 0 && !s.before)) // an empty first list is the legacy wildcard
		s.before = s.before || len(ns) > 0
		clear(s.names)
		for _, n := range ns {
			s.names[n] = true
		}
	case *discoveryv3.DeltaDiscoveryRequest:
		sub, unsub := req.GetResourceNamesSubscribe(), req.GetResourceNamesUnsubscribe()
		legacy := !s.before && len(sub) == 0 && len(unsub) == 0
		s.before = true
		s.all = wild && ((s.all && !slices.Contains(unsub, "*")) || legacy || slices.Contains(sub, "*")) // subscribing wins
		for _, n := range unsub {
			delete(s.names, n)
		}
		for _, n := range sub {
			s.names[n] = true
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
