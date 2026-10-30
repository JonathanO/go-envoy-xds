package xds_test

import (
	"context"
	"fmt"
	"log"
	"net"

	clusterv3 "github.com/envoyproxy/go-control-plane/envoy/config/cluster/v3"
	corev3 "github.com/envoyproxy/go-control-plane/envoy/config/core/v3"
	discoveryv3 "github.com/envoyproxy/go-control-plane/envoy/service/discovery/v3"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	xds "github.com/JonathanO/go-envoy-xds"
	"github.com/JonathanO/go-envoy-xds/snapshot"
)

// A Server serving a snapshot Source over gRPC, and a client playing an Envoy against it.
func ExampleNewServer() {
	// Every Envoy sees the view "all", of one group.
	src := snapshot.New[string, string, string](func(nodeID string) string { return "all" })
	src.Update(func(c *snapshot.Change[string, string]) {
		c.SetView("all", "clusters")
		c.EditGroup("clusters", func(e *snapshot.Editor) { e.Put(&clusterv3.Cluster{Name: "backend"}) })
	})

	srv, err := xds.NewServer(xds.ServerConfig[*snapshot.Update, string]{
		Source:   src,
		Builders: snapshot.Builders[string](xds.ClusterType, xds.EndpointType, xds.ListenerType, xds.RouteType),
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

	// Play an Envoy: ask for every cluster, over SotW.
	conn, err := grpc.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()
	stream, err := discoveryv3.NewAggregatedDiscoveryServiceClient(conn).StreamAggregatedResources(context.Background())
	if err != nil {
		log.Fatal(err)
	}
	recv := func() *discoveryv3.DiscoveryResponse {
		resp, err := stream.Recv()
		if err != nil {
			log.Fatal(err)
		}
		var names []string
		for _, a := range resp.GetResources() {
			var c clusterv3.Cluster
			if err := a.UnmarshalTo(&c); err != nil {
				log.Fatal(err)
			}
			names = append(names, c.GetName())
		}
		fmt.Println("version", resp.GetVersionInfo(), names)
		return resp
	}
	if err := stream.Send(&discoveryv3.DiscoveryRequest{Node: &corev3.Node{Id: "envoy-1"}, TypeUrl: string(xds.ClusterType)}); err != nil {
		log.Fatal(err)
	}
	first := recv()

	// A change waits for the ACK of what came before it.
	src.Update(func(c *snapshot.Change[string, string]) {
		c.EditGroup("clusters", func(e *snapshot.Editor) { e.Put(&clusterv3.Cluster{Name: "api"}) })
	})
	ack := &discoveryv3.DiscoveryRequest{TypeUrl: string(xds.ClusterType), VersionInfo: first.GetVersionInfo(), ResponseNonce: first.GetNonce()}
	if err := stream.Send(ack); err != nil {
		log.Fatal(err)
	}
	recv()
	// Output:
	// version 1 [backend]
	// version 2 [api backend]
}
