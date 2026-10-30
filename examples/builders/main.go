// Command builders is a toy control plane, not for production. Add services on stdin, and it serves Envoy a cluster,
// endpoints and a virtual host for each.
//
// It builds Envoy's config from state of its own, with a ConfigBuilder per type. ../snapshot is the same CLI on
// package snapshot, which does that work for you.
package main

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"log"
	"maps"
	"net"
	"net/netip"
	"os"
	"slices"
	"strings"

	corev3 "github.com/envoyproxy/go-control-plane/envoy/config/core/v3"
	discoveryv3 "github.com/envoyproxy/go-control-plane/envoy/service/discovery/v3"
	"google.golang.org/grpc"

	xds "github.com/JonathanO/go-envoy-xds"
)

func main() {
	m := newClusterManager()
	srv, err := newServer(m)
	if err != nil {
		log.Fatal(err)
	}
	lis, err := net.Listen("tcp", ":6566")
	if err != nil {
		log.Fatal(err)
	}
	// Serve it however you serve gRPC.
	gs := grpc.NewServer()
	discoveryv3.RegisterAggregatedDiscoveryServiceServer(gs, srv)
	go func() {
		if err := gs.Serve(lis); err != nil {
			log.Fatal(err)
		}
	}()
	cli(m)
	gs.Stop()
}

func newServer(m *clusterManager) (*xds.Server[update, int64], error) {
	return xds.NewServer(xds.ServerConfig[update, int64]{
		Source:    m, // tracks the services, and tells each Envoy when they change
		Builders:  []xds.ConfigBuilder[update, int64]{clusterBuilder{}, endpointBuilder{}, routeBuilder{}},
		ConfigFor: configFor,
	})
}

// configFor gives the builders the port an Envoy listens on, for its route config. It's in the node's metadata, as
// envoy-delta.yaml sets it. A real one would look up much more.
func configFor(_ context.Context, node *corev3.Node) (xds.EnvoyConfig[int64], error) {
	port := int64(8080)
	if v, ok := node.GetMetadata().GetFields()["my_listen_port"]; ok {
		port = int64(v.GetNumberValue())
	}
	return xds.EnvoyConfig[int64]{Details: port}, nil
}

const help = `Commands:
  list - list all service names
  show <name> - show endpoints for service by name
  add <name> <ip:port> - add a new endpoint to a service, creating it if needed
  remove <name> <ip:port> - remove an endpoint from a service
  remove <name> - remove a service
  quit - shutdown`

func cli(m *clusterManager) {
	fmt.Println("CLI driven service mesh!")
	fmt.Println(help)
	in := bufio.NewScanner(os.Stdin)
	for fmt.Print("> "); in.Scan(); fmt.Print("> ") {
		args := strings.Fields(in.Text())
		if len(args) == 0 {
			continue
		}
		if args[0] == "quit" {
			return
		}
		if err := command(m, args); err != nil {
			fmt.Println(err)
			fmt.Println(help)
		}
	}
}

func command(m *clusterManager, args []string) error {
	switch {
	case args[0] == "list" && len(args) == 1:
		fmt.Println("Services:")
		for _, name := range slices.Sorted(maps.Keys(m.state())) {
			fmt.Printf("\t%s\n", name)
		}
	case args[0] == "show" && len(args) == 2:
		fmt.Println("Service:", args[1])
		s, ok := m.state()[args[1]]
		if !ok {
			fmt.Println("\tNot Found")
		}
		for _, e := range s.endpoints {
			fmt.Printf("\t%s\n", e)
		}
	case args[0] == "add" && len(args) == 3:
		e, err := netip.ParseAddrPort(args[2])
		if err != nil {
			return err
		}
		m.addEndpoint(args[1], e)
	case args[0] == "remove" && len(args) == 3:
		e, err := netip.ParseAddrPort(args[2])
		if err != nil {
			return err
		}
		m.removeEndpoint(args[1], e)
	case args[0] == "remove" && len(args) == 2:
		m.removeService(args[1])
	case args[0] == "help" && len(args) == 1:
		fmt.Println(help)
	default:
		return errors.New("unknown command, or the wrong number of arguments")
	}
	return nil
}
