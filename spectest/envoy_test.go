package spectest

import (
	"context"
	"io"
	"sync"

	discoveryv3 "github.com/envoyproxy/go-control-plane/envoy/service/discovery/v3"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"

	xds "github.com/JonathanO/go-envoy-xds"
)

// An envoy is one Envoy in a vector, played by the runner.
type envoy struct {
	name string
	conn *conn // its stream, if it has one open
}

// A conn is an Envoy's stream to the Server, and what the Envoy knows of it.
type conn struct {
	send      func(proto.Message) error
	take      func() ([]*received, error) // the responses since the last call
	closeSend func()
	ended     chan error // what the Server's handler returned
	closing   bool       // the Envoy closed its side

	first bool                        // its next request is its first
	got   map[xds.TypeURL][]*received // the responses of each type, in order
	acked map[xds.TypeURL]string      // SotW: the version last ACKed of each type
	names map[xds.TypeURL][]string    // SotW: the names wanted of each type
}

// A received response, as a vector compares it, with what a request needs to answer it.
type received struct {
	got
	version, nonce string
}

func dial[Req, Resp any](ctx context.Context, serve func(grpc.BidiStreamingServer[Req, Resp]) error,
	convert func(*Resp) (*received, error)) *conn {
	s := &stream[Req, Resp]{ctx: ctx, reqs: make(chan *Req)}
	c := &conn{ended: make(chan error, 1), first: true, got: map[xds.TypeURL][]*received{},
		acked: map[xds.TypeURL]string{}, names: map[xds.TypeURL][]string{}}
	go func() { c.ended <- serve(s) }()
	c.send = func(m proto.Message) error {
		select {
		case s.reqs <- any(m).(*Req):
			return nil
		case err := <-c.ended:
			c.ended <- err
			return err
		}
	}
	c.take = func() ([]*received, error) {
		s.mu.Lock()
		sent := s.sent
		s.sent = nil
		s.mu.Unlock()
		var rs []*received
		for _, resp := range sent {
			r, err := convert(resp)
			if err != nil {
				return nil, err
			}
			rs = append(rs, r)
			c.got[r.typ] = append(c.got[r.typ], r)
		}
		return rs, nil
	}
	c.closeSend = func() { close(s.reqs) }
	return c
}

// back is the response of a type n before the last on the stream, or nil if there isn't one.
func (c *conn) back(typ xds.TypeURL, n int) *received {
	got := c.got[typ]
	if n >= len(got) {
		return nil
	}
	return got[len(got)-1-n]
}

func sotwReceived(r *discoveryv3.DiscoveryResponse) (*received, error) {
	g := newGot(xds.TypeURL(r.GetTypeUrl()), nil)
	for _, a := range r.GetResources() {
		m, err := a.UnmarshalNew()
		if err != nil {
			return nil, err
		}
		g.add(nameOf(m), m)
	}
	return &received{got: g, version: r.GetVersionInfo(), nonce: r.GetNonce()}, nil
}

func deltaReceived(r *discoveryv3.DeltaDiscoveryResponse) (*received, error) {
	g := newGot(xds.TypeURL(r.GetTypeUrl()), r.GetRemovedResources())
	for _, res := range r.GetResources() {
		m, err := res.GetResource().UnmarshalNew()
		if err != nil {
			return nil, err
		}
		g.add(res.GetName(), m)
	}
	return &received{got: g, version: r.GetSystemVersionInfo(), nonce: r.GetNonce()}, nil
}

// stream is the Server's side of a fake gRPC stream.
type stream[Req, Resp any] struct {
	grpc.ServerStream
	ctx  context.Context
	reqs chan *Req // closed when the Envoy closes its side
	mu   sync.Mutex
	sent []*Resp
}

func (s *stream[Req, Resp]) Context() context.Context { return s.ctx }

func (s *stream[Req, Resp]) Recv() (*Req, error) {
	select {
	case r, ok := <-s.reqs:
		if !ok {
			return nil, io.EOF
		}
		return r, nil
	case <-s.ctx.Done():
		return nil, s.ctx.Err()
	}
}

func (s *stream[Req, Resp]) Send(r *Resp) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.sent = append(s.sent, r)
	return nil
}
