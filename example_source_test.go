package xds_test

import (
	"context"
	"fmt"
	"log"
	"net"
	"sync"
	"time"

	clusterv3 "github.com/envoyproxy/go-control-plane/envoy/config/cluster/v3"
	corev3 "github.com/envoyproxy/go-control-plane/envoy/config/core/v3"
	discoveryv3 "github.com/envoyproxy/go-control-plane/envoy/service/discovery/v3"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/proto"

	xds "github.com/JonathanO/go-envoy-xds"
)

// clusterSet is the whole config: clusters by name.
type clusterSet map[string]*clusterv3.Cluster

// A clusterChange is the state update: the config before and after.
type clusterChange struct{ before, after clusterSet }

// clusterSource holds the config, and tells each Envoy's stream when it changes.
type clusterSource struct {
	mu      sync.Mutex
	current clusterSet
	streams map[*func(clusterChange)]struct{}
}

func (s *clusterSource) Subscribe(_ string, deliver func(clusterChange)) (clusterChange, func()) {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := &deliver
	s.streams[key] = struct{}{}
	return clusterChange{after: s.current}, func() {
		s.mu.Lock()
		defer s.mu.Unlock()
		delete(s.streams, key)
	}
}

// Set publishes new config. deliver never blocks, so calling it with the lock held keeps every stream's updates in
// order, and none is missed by a stream subscribing meanwhile.
func (s *clusterSource) Set(next clusterSet) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for deliver := range s.streams {
		(*deliver)(clusterChange{before: s.current, after: next})
	}
	s.current = next
}

// clusterBuilder serves clusters from clusterChanges.
type clusterBuilder struct{}

func (clusterBuilder) Type() xds.TypeURL { return xds.ClusterType }

// AddOrder is what's new or changed.
func (clusterBuilder) AddOrder(c clusterChange, subscribed func(string) bool, _ string) xds.Response {
	var r xds.Response
	for name, cl := range c.after {
		if subscribed(name) && !proto.Equal(cl, c.before[name]) {
			r.Updates = append(r.Updates, xds.Resource{Name: name, Message: cl})
		}
	}
	return r
}

// RemoveOrder is what's gone.
func (clusterBuilder) RemoveOrder(c clusterChange, subscribed func(string) bool, _ string) xds.Response {
	var r xds.Response
	for name := range c.before {
		if _, ok := c.after[name]; !ok && subscribed(name) {
			r.Removes = append(r.Removes, name)
		}
	}
	return r
}

// ResourcesAddOrder is everything mid-change: what's new is in, and what's gone hasn't gone yet.
func (b clusterBuilder) ResourcesAddOrder(c clusterChange, subscribed func(string) bool, _ string) []xds.Resource {
	rs := b.ResourcesRemoveOrder(c, subscribed, "")
	for name, cl := range c.before {
		if _, ok := c.after[name]; !ok && subscribed(name) {
			rs = append(rs, xds.Resource{Name: name, Message: cl})
		}
	}
	return rs
}

// ResourcesRemoveOrder is everything once the change is done.
func (clusterBuilder) ResourcesRemoveOrder(c clusterChange, subscribed func(string) bool, _ string) []xds.Resource {
	var rs []xds.Resource
	for name, cl := range c.after {
		if subscribed(name) {
			rs = append(rs, xds.Resource{Name: name, Message: cl})
		}
	}
	return rs
}

// A Source and ConfigBuilder of your own, serving a delta Envoy. Replacing cluster a with b goes in two rounds: b is
// added, and once Envoy ACKs that, a is removed.
func Example_customSource() {
	src := &clusterSource{current: clusterSet{"a": {Name: "a"}}, streams: map[*func(clusterChange)]struct{}{}}
	srv, err := xds.NewServer(xds.ServerConfig[clusterChange, string]{
		Source:   src,
		Builders: []xds.ConfigBuilder[clusterChange, string]{clusterBuilder{}},
		ConfigFor: func(_ context.Context, node *corev3.Node) (xds.EnvoyConfig[string], error) {
			return xds.EnvoyConfig[string]{Details: node.GetId()}, nil
		},
	})
	if err != nil {
		log.Fatal(err)
	}
	grpcServer := grpc.NewServer()
	discoveryv3.RegisterAggregatedDiscoveryServiceServer(grpcServer, srv)
	lis, err := net.Listen("tcp", "localhost:0")
	if err != nil {
		log.Fatal(err)
	}
	go grpcServer.Serve(lis)
	defer grpcServer.Stop()

	// Play an Envoy: subscribe to every cluster, over delta, and ACK each response.
	conn, err := grpc.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	stream, err := discoveryv3.NewAggregatedDiscoveryServiceClient(conn).DeltaAggregatedResources(ctx)
	if err != nil {
		log.Fatal(err)
	}
	send := func(req *discoveryv3.DeltaDiscoveryRequest) {
		if err := stream.Send(req); err != nil {
			log.Fatal(err)
		}
	}
	recvAndAck := func() {
		resp, err := stream.Recv()
		if err != nil {
			log.Fatal(err)
		}
		var added []string
		for _, r := range resp.GetResources() {
			added = append(added, r.GetName())
		}
		fmt.Println("version", resp.GetSystemVersionInfo(), "added", added, "removed", resp.GetRemovedResources())
		send(&discoveryv3.DeltaDiscoveryRequest{TypeUrl: string(xds.ClusterType), ResponseNonce: resp.GetNonce()})
	}
	send(&discoveryv3.DeltaDiscoveryRequest{Node: &corev3.Node{Id: "envoy-1"}, TypeUrl: string(xds.ClusterType)})
	recvAndAck()

	src.Set(clusterSet{"b": {Name: "b"}})
	recvAndAck()
	recvAndAck()
	// Output:
	// version 1 added [a] removed []
	// version 2 added [b] removed []
	// version 3 added [] removed [a]
}
