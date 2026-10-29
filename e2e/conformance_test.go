// Runs the xDS conformance suite from envoy-xds-testing-go against go-envoy-xds, through package snapshot, on real
// Envoys. Needs Docker. ENVOY_VERSIONS picks the Envoy releases, e.g. "1.36,1.40"; by default every one upstream
// supports.
package e2e

import (
	"context"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	corev3 "github.com/envoyproxy/go-control-plane/envoy/config/core/v3"
	listenerv3 "github.com/envoyproxy/go-control-plane/envoy/config/listener/v3"
	routev3 "github.com/envoyproxy/go-control-plane/envoy/config/route/v3"
	discoveryv3 "github.com/envoyproxy/go-control-plane/envoy/service/discovery/v3"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"

	"github.com/JonathanO/envoy-xds-testing-go/conformance"
	xds "github.com/JonathanO/go-envoy-xds"
	"github.com/JonathanO/go-envoy-xds/snapshot"
)

// After a SotW reconnect, CDS's answer is the whole set, so a cluster Envoy kept and its kept routes still use is
// removed: we no longer have that cluster's config to send again.
const sotwReconnect = "known limit: after a SotW reconnect, a cluster Envoy kept is removed while its kept routes still use it"

func TestConformance(t *testing.T) {
	versions := "1.36,1.37,1.38,1.39,1.40"
	if v := os.Getenv("ENVOY_VERSIONS"); v != "" {
		versions = v
	}
	var images []string
	for v := range strings.SplitSeq(versions, ",") {
		images = append(images, "envoyproxy/envoy:v"+strings.TrimSpace(v)+"-latest")
	}
	conformance.Run(t, conformance.Options{
		New:         func(t *testing.T) conformance.ControlPlane { c := newControlPlane(); c.t = t; return c },
		EnvoyImages: images,
		Skip:        map[string]string{"sotw/ChangesWhileDisconnected": sotwReconnect, "sotw/ControlPlaneRestarts": sotwReconnect},
	})
}

// controlPlane serves every Envoy one view of two groups, so real Envoys check that a view merges its groups.
type controlPlane struct {
	t   *testing.T
	src *snapshot.Source[string, string, string]

	mu      sync.Mutex
	clients []*xds.Client[string]
	streams []*idleness
}

func newControlPlane() *controlPlane {
	src := snapshot.New[string, string, string](func(string) string { return "envoy" })
	src.Update(func(c *snapshot.Change[string, string]) { c.SetView("envoy", "routing", "clusters") })
	return &controlPlane{src: src}
}

func (c *controlPlane) Register(s *grpc.Server) {
	srv, err := xds.NewServer(xds.ServerConfig[*snapshot.Update, string]{
		Source: c.src,
		Builders: snapshot.Builders[string](xds.ClusterType, xds.EndpointType, xds.ListenerType, xds.RouteType,
			xds.ScopedRouteType, xds.VirtualHostType),
		// Every scenario's Envoy uses these; only some use scoped routes or VHDS, so stale clusters after a reconnect
		// mustn't wait for those.
		ConfigFor: func(_ context.Context, n *corev3.Node) (xds.EnvoyConfig[string], error) {
			return xds.EnvoyConfig[string]{Details: n.GetId(),
				SubscribesTo: []xds.TypeURL{xds.ListenerType, xds.RouteType, xds.ClusterType, xds.EndpointType}}, nil
		},
		Events: xds.Events[string]{Connected: func(cl *xds.Client[string]) {
			c.mu.Lock()
			defer c.mu.Unlock()
			c.clients = append(c.clients, cl)
		}},
		Metrics: func() xds.Metrics {
			m := &idleness{}
			m.touch()
			c.mu.Lock()
			defer c.mu.Unlock()
			c.streams = append(c.streams, m)
			return m
		},
	})
	if err != nil {
		panic(err)
	}
	discoveryv3.RegisterAggregatedDiscoveryServiceServer(s, &logged{srv, c.t})
}

func (c *controlPlane) SetConfig(resources []proto.Message) {
	routing, clusters := snapshot.Empty().Edit(), snapshot.Empty().Edit()
	for _, r := range resources {
		switch r.(type) {
		case *listenerv3.Listener, *routev3.RouteConfiguration, *routev3.ScopedRouteConfiguration, *routev3.VirtualHost:
			routing.Put(r)
		default:
			clusters.Put(r)
		}
	}
	c.src.Update(func(ch *snapshot.Change[string, string]) {
		ch.SetGroup("routing", routing.Build())
		ch.SetGroup("clusters", clusters.Build())
	})
}

func (c *controlPlane) Idle() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.streams) == 0 || c.streams[len(c.streams)-1].idle()
}

func (c *controlPlane) Resync() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.clients[len(c.clients)-1].Resync()
}

// idleness tracks whether a stream has a push under way or ACKs outstanding.
type idleness struct {
	pushing     atomic.Bool
	outstanding atomic.Int64
	last        atomic.Int64 // UnixNano of the last event
}

// Between one push completing and the next beginning, it reads idle for an instant.
const quiet = 100 * time.Millisecond

func (m *idleness) touch()         { m.last.Store(time.Now().UnixNano()) }
func (m *idleness) PushBegan()     { m.touch(); m.pushing.Store(true) }
func (m *idleness) PushCompleted() { m.touch(); m.pushing.Store(false) }
func (m *idleness) AwaitingAck()   { m.touch(); m.outstanding.Add(1) }
func (m *idleness) Acked()         { m.touch(); m.outstanding.Add(-1) }
func (m *idleness) Closed()        { m.PushCompleted() }

func (m *idleness) idle() bool {
	return !m.pushing.Load() && m.outstanding.Load() == 0 && time.Since(time.Unix(0, m.last.Load())) >= quiet
}

type logged struct {
	*xds.Server[*snapshot.Update, string]
	t *testing.T
}

func (l *logged) DeltaAggregatedResources(s grpc.BidiStreamingServer[discoveryv3.DeltaDiscoveryRequest, discoveryv3.DeltaDiscoveryResponse]) error {
	return l.Server.DeltaAggregatedResources(&loggedStream{s, l.t})
}

type loggedStream struct {
	grpc.BidiStreamingServer[discoveryv3.DeltaDiscoveryRequest, discoveryv3.DeltaDiscoveryResponse]
	t *testing.T
}

func short(t string) string { return t[strings.LastIndex(t, ".")+1:] }

func (s *loggedStream) Recv() (*discoveryv3.DeltaDiscoveryRequest, error) {
	r, err := s.BidiStreamingServer.Recv()
	if err == nil {
		s.t.Logf("%s REQ %s nonce=%q +%v -%v kept=%v err=%v", time.Now().Format("15:04:05.000"), short(r.GetTypeUrl()), r.GetResponseNonce(), r.GetResourceNamesSubscribe(), r.GetResourceNamesUnsubscribe(), r.GetInitialResourceVersions(), r.GetErrorDetail() != nil)
	}
	return r, err
}

func (s *loggedStream) Send(r *discoveryv3.DeltaDiscoveryResponse) error {
	var ns []string
	for _, x := range r.GetResources() {
		ns = append(ns, x.GetName())
	}
	s.t.Logf("%s RESP %s nonce=%q %v removed=%v", time.Now().Format("15:04:05.000"), short(r.GetTypeUrl()), r.GetNonce(), ns, r.GetRemovedResources())
	return s.BidiStreamingServer.Send(r)
}
