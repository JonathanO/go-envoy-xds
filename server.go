// Package xds serves config to Envoys over ADS, SotW or delta, in make-before-break order: whatever a change adds is
// in place before anything that refers to it, and nothing is removed while something still refers to it.
//
// A Server takes state updates from a Source, and turns each into resources with a ConfigBuilder per type. Package
// snapshot provides both, from named groups of resources.
package xds

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"

	corev3 "github.com/envoyproxy/go-control-plane/envoy/config/core/v3"
	discoveryv3 "github.com/envoyproxy/go-control-plane/envoy/service/discovery/v3"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// ServerConfig configures a Server: Source and ConfigFor are required.
type ServerConfig[U, D any] struct {
	// Source tells each Envoy's stream about changes.
	Source Source[U, D]
	// Builders turn updates into resources, one per type served. Types with none have no resources.
	Builders []ConfigBuilder[U, D]
	// ConfigFor gives an Envoy's config from the node in its first request. An error ends the stream with it.
	ConfigFor func(ctx context.Context, node *corev3.Node) (EnvoyConfig[D], error)
	// Events are told what Envoys do.
	Events Events[D]
	// Metrics, if set, makes the Metrics for each stream.
	Metrics func() Metrics
	// Logger defaults to slog.Default().
	Logger *slog.Logger
}

// A Server is an ADS service: register it with discoveryv3.RegisterAggregatedDiscoveryServiceServer.
type Server[U, D any] struct {
	source    Source[U, D]
	builders  map[TypeURL]ConfigBuilder[U, D]
	configFor func(ctx context.Context, node *corev3.Node) (EnvoyConfig[D], error)
	events    Events[D]
	metrics   func() Metrics
	log       *slog.Logger
}

var _ discoveryv3.AggregatedDiscoveryServiceServer = (*Server[any, any])(nil)

// NewServer returns a Server, or an error if the config is incomplete or a builder's type is unknown or repeated.
func NewServer[U, D any](c ServerConfig[U, D]) (*Server[U, D], error) {
	if c.Source == nil || c.ConfigFor == nil {
		return nil, errors.New("xds: ServerConfig needs a Source and ConfigFor")
	}
	s := &Server[U, D]{source: c.Source, builders: map[TypeURL]ConfigBuilder[U, D]{}, configFor: c.ConfigFor, events: c.Events,
		metrics: c.Metrics, log: cmp.Or(c.Logger, slog.Default())}
	for _, b := range c.Builders {
		t := b.Type()
		if !t.known() {
			return nil, fmt.Errorf("xds: can't serve %s: it's not one of the ADS types", t)
		}
		if _, ok := s.builders[t]; ok {
			return nil, fmt.Errorf("xds: two builders for %s", t)
		}
		s.builders[t] = b
	}
	if s.metrics == nil {
		s.metrics = func() Metrics { return noMetrics{} }
	}
	return s, nil
}

// StreamAggregatedResources serves a SotW stream.
func (s *Server[U, D]) StreamAggregatedResources(stream grpc.BidiStreamingServer[discoveryv3.DiscoveryRequest, discoveryv3.DiscoveryResponse]) error {
	return serve(s, stream, func(send func(*discoveryv3.DiscoveryResponse), fail func(error), cfg EnvoyConfig[D]) map[TypeURL]service[*discoveryv3.DiscoveryRequest, U] {
		services := map[TypeURL]service[*discoveryv3.DiscoveryRequest, U]{}
		for _, t := range addOrder {
			var subs sotwSubs = &sotwNames{}
			if t.Wildcard() {
				subs = &sotwWildcard{}
			}
			services[t] = &sotwService[U, D]{base: s.base(t, cfg), send: send, fail: fail, subs: subs, have: map[string]Resource{}}
		}
		return services
	})
}

// DeltaAggregatedResources serves a delta stream.
func (s *Server[U, D]) DeltaAggregatedResources(stream grpc.BidiStreamingServer[discoveryv3.DeltaDiscoveryRequest, discoveryv3.DeltaDiscoveryResponse]) error {
	return serve(s, stream, func(send func(*discoveryv3.DeltaDiscoveryResponse), fail func(error), cfg EnvoyConfig[D]) map[TypeURL]service[*discoveryv3.DeltaDiscoveryRequest, U] {
		services := map[TypeURL]service[*discoveryv3.DeltaDiscoveryRequest, U]{}
		for _, t := range addOrder {
			var subs deltaSubs = &deltaNames{}
			if t.Wildcard() {
				subs = &deltaWildcard{}
			}
			svc := &deltaService[U, D]{base: s.base(t, cfg), send: send, fail: fail, subs: subs,
				answeredDuringChange: set{}}
			svc.deferred = set{} // only delta has stale names to defer
			services[t] = svc
		}
		return services
	})
}

func (s *Server[U, D]) base(t TypeURL, cfg EnvoyConfig[D]) base[U, D] {
	return base[U, D]{typ: t, builder: s.builders[t], details: cfg.Details, log: s.log}
}

// errResync ends a stream for Client.Resync.
var errResync = errors.New("xds: resync")

type adsStream[R request, S any] interface {
	Context() context.Context
	Recv() (R, error)
	Send(S) error
}

// serve runs one stream. Its goroutine owns the stream's state, so the only locking is in the mailbox.
func serve[R request, S any, U, D any](srv *Server[U, D], stream adsStream[R, S],
	newServices func(send func(S), fail func(error), cfg EnvoyConfig[D]) map[TypeURL]service[R, U]) (err error) {
	ctx, cancel := context.WithCancelCause(stream.Context())
	defer cancel(nil)

	var (
		client *Client[D]
		m      *manager[R, U]
		ready  <-chan struct{} // the mailbox's, once there's a manager
	)
	// A panic, from a builder say, ends this stream alone, after the cleanup below.
	defer func() {
		if p := recover(); p != nil {
			var id string
			if client != nil {
				id = client.Node.GetId()
			}
			srv.log.Error("Panicked serving an Envoy, so closing its stream", "node", id, "panic", p)
			err = status.Error(codes.Internal, "xds: internal error")
		}
	}()

	requests := make(chan R)
	go func() {
		for {
			req, err := stream.Recv()
			if err != nil {
				cancel(err)
				return
			}
			select {
			case requests <- req:
			case <-ctx.Done():
				return
			}
		}
	}()
	send := func(resp S) {
		if err := stream.Send(resp); err != nil {
			cancel(err)
		}
	}

	defer func() {
		if m != nil {
			m.close()
		}
		if client != nil && srv.events.Disconnected != nil {
			srv.events.Disconnected(client)
		}
	}()
	handle := func(req R) error {
		if client == nil {
			cfg, err := srv.configFor(ctx, req.GetNode())
			if err != nil {
				return err
			}
			client = &Client[D]{Node: req.GetNode(), Config: cfg, resync: func() { cancel(errResync) }}
			if srv.events.Connected != nil {
				srv.events.Connected(client)
			}
		}
		t := TypeURL(req.GetTypeUrl())
		if !t.known() {
			// Leave it unanswered, like a type nobody serves: ending the stream would take every other type down with
			// it, and Envoy would reconnect and ask again.
			srv.log.Warn("Ignoring a request for an unknown type", "node", client.Node.GetId(), "type", t)
			return nil
		}
		if detail := req.GetErrorDetail(); detail != nil {
			srv.log.Warn("Envoy rejected config, so sending it no more changes on this stream", "node", client.Node.GetId(),
				"type", t, "error", detail.GetMessage())
			if srv.events.Nacked != nil {
				srv.events.Nacked(client, t, detail)
			}
		}
		if m == nil {
			mb := newMailbox[U]()
			m = newManager(newServices(send, func(err error) { cancel(err) }, client.Config), client.Config, mb, srv.metrics(), srv.log)
			m.init(srv.source.Subscribe(client.Config.Details, mb.put))
			ready = mb.ready
		}
		if err := m.process(req); err != nil {
			return status.Error(codes.InvalidArgument, err.Error())
		}
		return nil
	}
	for {
		select {
		case <-ctx.Done():
			return srv.ended(client, context.Cause(ctx))
		case <-ready:
			m.updatesWaiting()
		case req := <-requests:
			if err := handle(req); err != nil {
				return srv.ended(client, err)
			}
		}
	}
}

// ended is what serve returns when the stream ends with err.
func (s *Server[U, D]) ended(c *Client[D], err error) error {
	switch {
	case errors.Is(err, errResync):
		s.log.Info("Closing stream so it resyncs", "node", c.Node.GetId())
		return nil // Envoy reconnects
	case errors.Is(err, io.EOF):
		return nil
	case !errors.Is(err, context.Canceled) && status.Code(err) != codes.Canceled: // not Envoy going away
		s.log.Warn("Error streaming to an ADS client", "error", err)
	}
	return err
}
