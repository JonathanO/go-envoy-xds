// Command e2e is go-envoy-xds's conformance server, for the real-Envoy conformance suite in envoy-xds-spec
// (spec/conformance). It serves ADS from xds.Server over package snapshot, and the suite's control API, as
// spec/docs/CONFORMANCE.md describes. The harness starts it: build it with go build, then run the suite with
// ./gradlew conformance -Pserver=/path/to/it in spec/conformance.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"sync"
	"sync/atomic"
	"time"

	clusterv3 "github.com/envoyproxy/go-control-plane/envoy/config/cluster/v3"
	corev3 "github.com/envoyproxy/go-control-plane/envoy/config/core/v3"
	endpointv3 "github.com/envoyproxy/go-control-plane/envoy/config/endpoint/v3"
	listenerv3 "github.com/envoyproxy/go-control-plane/envoy/config/listener/v3"
	routev3 "github.com/envoyproxy/go-control-plane/envoy/config/route/v3"
	discoveryv3 "github.com/envoyproxy/go-control-plane/envoy/service/discovery/v3"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"

	xds "github.com/JonathanO/go-envoy-xds"
	"github.com/JonathanO/go-envoy-xds/snapshot"
)

// After a SotW reconnect, CDS's answer is the whole set, so a cluster Envoy kept and its kept routes still use is
// removed: we no longer have that cluster's config to send again.
const sotwReconnect = "known limit: after a SotW reconnect, a cluster Envoy kept is removed while its kept routes still use it"

// deviations are the suite's scenarios this port is known to fail, by path, with why.
var deviations = map[string]string{"sotw/changesWhileDisconnected": sotwReconnect, "sotw/controlPlaneRestarts": sotwReconnect}

func main() {
	if err := run(os.Stdin, os.Stdout); err != nil {
		log.Fatal(err)
	}
}

// run serves until in closes: the harness holds it open while it needs the server.
func run(in io.Reader, out io.Writer) error {
	s, err := newServer()
	if err != nil {
		return err
	}
	ads, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return err
	}
	control, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return err
	}
	g := grpc.NewServer()
	discoveryv3.RegisterAggregatedDiscoveryServiceServer(g, s.ads)
	go func() { _ = g.Serve(ads) }()
	go func() { _ = http.Serve(control, s) }()
	if err := json.NewEncoder(out).Encode(map[string]any{
		"ads": ads.Addr().(*net.TCPAddr).Port, "control": control.Addr().(*net.TCPAddr).Port, "deviations": deviations,
	}); err != nil {
		return err
	}
	_, _ = io.Copy(io.Discard, in)
	return nil
}

type change = snapshot.Change[string, string]

// server is an xds.Server over a snapshot.Source, whose views are named by node ID, and its control API.
type server struct {
	src *snapshot.Source[string, string, string]
	ads *xds.Server[*snapshot.Update, string]

	mu      sync.Mutex
	clients map[string]*xds.Client[string] // the latest by node ID
	streams []*idleness
}

func newServer() (*server, error) {
	s := &server{src: snapshot.New[string, string, string](func(node string) string { return node }), clients: map[string]*xds.Client[string]{}}
	var err error
	s.ads, err = xds.NewServer(xds.ServerConfig[*snapshot.Update, string]{
		Source: s.src,
		Builders: snapshot.Builders[string](xds.ClusterType, xds.EndpointType, xds.ListenerType, xds.RouteType,
			xds.ScopedRouteType, xds.VirtualHostType),
		// Every scenario's Envoy uses these; only some use scoped routes or VHDS, so stale clusters after a reconnect
		// mustn't wait for those.
		ConfigFor: func(_ context.Context, n *corev3.Node) (xds.EnvoyConfig[string], error) {
			return xds.EnvoyConfig[string]{Details: n.GetId(),
				SubscribesTo: []xds.TypeURL{xds.ListenerType, xds.RouteType, xds.ClusterType, xds.EndpointType}}, nil
		},
		Events: xds.Events[string]{Connected: func(cl *xds.Client[string]) {
			s.mu.Lock()
			defer s.mu.Unlock()
			s.clients[cl.Node.GetId()] = cl
		}},
		Metrics: func() xds.Metrics {
			m := &idleness{}
			m.touch()
			s.mu.Lock()
			defer s.mu.Unlock()
			s.streams = append(s.streams, m)
			return m
		},
	})
	return s, err
}

// ServeHTTP serves the control API.
func (s *server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	var err error
	switch r.Method + " " + r.URL.Path {
	case "POST /update":
		var ops []json.RawMessage
		if err = json.NewDecoder(r.Body).Decode(&ops); err == nil {
			err = s.update(ops)
		}
	case "POST /resync":
		var body struct{ Node string }
		if err = json.NewDecoder(r.Body).Decode(&body); err == nil {
			err = s.resync(body.Node)
		}
	case "GET /idle":
		w.Header().Set("Content-Type", "application/json")
		err = json.NewEncoder(w).Encode(map[string]bool{"idle": s.idle()})
	default:
		http.NotFound(w, r)
		return
	}
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
	}
}

// op is one of an update's operations, as in a trace vector, but with each resource its protobuf encoding.
type op struct {
	SetView     string              `json:"setView"`
	Groups      []string            `json:"groups"`
	EditGroup   string              `json:"editGroup"`
	Put         map[string][][]byte `json:"put"`
	Remove      map[string][]string `json:"remove"`
	SetGroup    string              `json:"setGroup"`
	Resources   map[string][][]byte `json:"resources"`
	RemoveGroup string              `json:"removeGroup"`
}

// messages are the types served, by their name in the control API.
var messages = map[string]proto.Message{
	"LDS":  &listenerv3.Listener{},
	"RDS":  &routev3.RouteConfiguration{},
	"SRDS": &routev3.ScopedRouteConfiguration{},
	"VHDS": &routev3.VirtualHost{},
	"CDS":  &clusterv3.Cluster{},
	"EDS":  &endpointv3.ClusterLoadAssignment{},
}

func parse(byType map[string][][]byte) ([]proto.Message, error) {
	var all []proto.Message
	for typ, resources := range byType {
		m, ok := messages[typ]
		if !ok {
			return nil, fmt.Errorf("unknown type %q", typ)
		}
		for _, b := range resources {
			r := m.ProtoReflect().New().Interface()
			if err := proto.Unmarshal(b, r); err != nil {
				return nil, fmt.Errorf("not a %s resource: %w", typ, err)
			}
			all = append(all, r)
		}
	}
	return all, nil
}

// update makes one Source update. Resources are parsed first, as the Source is locked while the update runs.
func (s *server) update(ops []json.RawMessage) error {
	var edits []func(*change)
	for _, raw := range ops {
		var then string
		if json.Unmarshal(raw, &then) == nil {
			if then != "then" {
				return fmt.Errorf("unknown update op %s", raw)
			}
			edits = append(edits, (*change).Then)
			continue
		}
		var o op
		if err := json.Unmarshal(raw, &o); err != nil {
			return fmt.Errorf("update op %s: %w", raw, err)
		}
		switch {
		case o.SetView != "":
			edits = append(edits, func(c *change) { c.SetView(o.SetView, o.Groups...) })
		case o.EditGroup != "":
			puts, err := parse(o.Put)
			if err != nil {
				return err
			}
			removes := map[xds.TypeURL][]string{}
			for typ, names := range o.Remove {
				m, ok := messages[typ]
				if !ok {
					return fmt.Errorf("unknown type %q", typ)
				}
				removes[xds.TypeOf(m)] = names
			}
			edits = append(edits, func(c *change) {
				c.EditGroup(o.EditGroup, func(e *snapshot.Editor) {
					for _, m := range puts {
						e.Put(m)
					}
					for typ, names := range removes {
						for _, n := range names {
							e.Remove(typ, n)
						}
					}
				})
			})
		case o.SetGroup != "":
			resources, err := parse(o.Resources)
			if err != nil {
				return err
			}
			e := snapshot.Empty().Edit()
			for _, m := range resources {
				e.Put(m)
			}
			group := e.Build()
			edits = append(edits, func(c *change) { c.SetGroup(o.SetGroup, group) })
		case o.RemoveGroup != "":
			edits = append(edits, func(c *change) { c.RemoveGroup(o.RemoveGroup) })
		default:
			return fmt.Errorf("unknown update op %s", raw)
		}
	}
	s.src.Update(func(c *change) {
		for _, edit := range edits {
			edit(c)
		}
	})
	return nil
}

func (s *server) resync(node string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	cl, ok := s.clients[node]
	if !ok {
		return errors.New("no Envoy has connected as " + node)
	}
	cl.Resync()
	return nil
}

// idle is whether the latest stream has nothing in flight or waiting to be sent.
func (s *server) idle() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.streams) == 0 || s.streams[len(s.streams)-1].idle()
}

// idleness tracks whether a stream has a push under way or ACKs outstanding.
type idleness struct {
	pushing     atomic.Bool
	outstanding atomic.Int64
	last        atomic.Int64 // UnixNano of the last event
}

// Between one push completing and the next beginning, it reads idle for an instant. A rate limited Envoy's next
// request, e.g. a subscription to what an ACKed change named, can also come a couple of hundred milliseconds after
// its ACK, so the quiet period is longer than that.
const quiet = 500 * time.Millisecond

func (m *idleness) touch()         { m.last.Store(time.Now().UnixNano()) }
func (m *idleness) PushBegan()     { m.touch(); m.pushing.Store(true) }
func (m *idleness) PushCompleted() { m.touch(); m.pushing.Store(false) }
func (m *idleness) AwaitingAck()   { m.touch(); m.outstanding.Add(1) }
func (m *idleness) Acked()         { m.touch(); m.outstanding.Add(-1) }
func (m *idleness) Frozen()        {}
func (m *idleness) Closed()        { m.PushCompleted() }

func (m *idleness) idle() bool {
	return !m.pushing.Load() && m.outstanding.Load() == 0 && time.Since(time.Unix(0, m.last.Load())) >= quiet
}
