// Command snapshot is the same toy control plane as ../builders, on package snapshot instead of ConfigBuilders of our
// own.
//
// We describe the config we want as groups of resources. The snapshot builders work out what changed, and send it make
// before break: a new cluster and its endpoints reach Envoy before the route that uses it, and a removed cluster goes
// only after the route has stopped using it.
//
// Every Envoy sees the "clusters" group: clusters and their endpoints. The route config depends on the port each Envoy
// listens on, so each port gets a group with its route config, and a view of that group and "clusters". Envoys on the
// same port share a view. The first to connect on a port makes it.
package main

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"log"
	"net"
	"net/netip"
	"os"
	"slices"
	"strconv"
	"strings"
	"time"

	clusterv3 "github.com/envoyproxy/go-control-plane/envoy/config/cluster/v3"
	corev3 "github.com/envoyproxy/go-control-plane/envoy/config/core/v3"
	endpointv3 "github.com/envoyproxy/go-control-plane/envoy/config/endpoint/v3"
	routev3 "github.com/envoyproxy/go-control-plane/envoy/config/route/v3"
	discoveryv3 "github.com/envoyproxy/go-control-plane/envoy/service/discovery/v3"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"

	xds "github.com/JonathanO/go-envoy-xds"
	"github.com/JonathanO/go-envoy-xds/snapshot"
)

const (
	routeName = "all_routes" // the route config the http connection manager in envoy-delta.yaml asks for
	clusters  = "clusters"
)

// A mesh is the services, as groups named by strings, in views named by the port their Envoys listen on.
type mesh struct {
	src   *snapshot.Source[int64, string, int64]
	ports map[int64]struct{} // those with a view; only used in Update's edit, which holds src's lock
}

func newMesh() *mesh {
	return &mesh{src: snapshot.New[int64, string, int64](func(port int64) int64 { return port }), ports: map[int64]struct{}{}}
}

func main() {
	m := newMesh()
	srv, err := xds.NewServer(xds.ServerConfig[*snapshot.Update, int64]{
		Source:    m.src, // tells each Envoy when its view changes
		Builders:  snapshot.Builders[int64](xds.ClusterType, xds.EndpointType, xds.RouteType),
		ConfigFor: configFor,
		Events:    xds.Events[int64]{Connected: func(c *xds.Client[int64]) { m.addPort(c.Config.Details) }},
	})
	if err != nil {
		log.Fatal(err)
	}
	lis, err := net.Listen("tcp", ":6566")
	if err != nil {
		log.Fatal(err)
	}
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

// configFor gives the port an Envoy listens on, which picks its view. It's in the node's metadata, as
// envoy-delta.yaml sets it.
func configFor(_ context.Context, node *corev3.Node) (xds.EnvoyConfig[int64], error) {
	port := int64(8080)
	if v, ok := node.GetMetadata().GetFields()["my_listen_port"]; ok {
		port = int64(v.GetNumberValue())
	}
	return xds.EnvoyConfig[int64]{Details: port}, nil
}

func routesGroup(port int64) string { return "routes-" + strconv.FormatInt(port, 10) }

// addPort gives a port its route config and view. Adding one again changes nothing.
func (m *mesh) addPort(port int64) {
	m.src.Update(func(c *snapshot.Change[string, int64]) {
		m.ports[port] = struct{}{}
		c.SetView(port, routesGroup(port), clusters)
		putRoutes(c, port)
	})
}

// changeClusters edits the clusters, and every port's routes to match, in one update.
func (m *mesh) changeClusters(edit func(e *snapshot.Editor)) {
	m.src.Update(func(c *snapshot.Change[string, int64]) {
		c.EditGroup(clusters, edit)
		for port := range m.ports {
			putRoutes(c, port)
		}
	})
}

// putRoutes puts the route config for Envoys on a port, with a virtual host per cluster.
func putRoutes(c *snapshot.Change[string, int64], port int64) {
	rc := &routev3.RouteConfiguration{Name: routeName}
	for _, name := range names(c.Group(clusters), xds.ClusterType) {
		rc.VirtualHosts = append(rc.VirtualHosts, &routev3.VirtualHost{
			Name:    name,
			Domains: []string{name, fmt.Sprintf("%s:%d", name, port)},
			Routes: []*routev3.Route{{
				Match:  &routev3.RouteMatch{PathSpecifier: &routev3.RouteMatch_Prefix{Prefix: "/"}},
				Action: &routev3.Route_Route{Route: &routev3.RouteAction{ClusterSpecifier: &routev3.RouteAction_Cluster{Cluster: name}}},
			}},
		})
	}
	c.EditGroup(routesGroup(port), func(e *snapshot.Editor) { e.Put(rc) })
}

// names are the names of a group's resources of a type, sorted, so the same clusters make an equal route config, which
// Put skips.
func names(g *snapshot.Snapshot, t xds.TypeURL) []string {
	var ns []string
	for name := range g.Resources(t) {
		ns = append(ns, name)
	}
	slices.Sort(ns)
	return ns
}

func (m *mesh) add(name string, endpoint netip.AddrPort) {
	added := lbEndpoint(endpoint)
	endpoints := m.endpoints(name)
	if slices.ContainsFunc(endpoints, func(e *endpointv3.LbEndpoint) bool { return proto.Equal(e, added) }) {
		return
	}
	// The cluster, its endpoints and the routes go in one update: Envoy gets the cluster before the route that uses
	// it. Putting a resource equal to the one there is a no-op, so putting the cluster again costs nothing.
	m.changeClusters(func(e *snapshot.Editor) {
		e.Put(cluster(name))
		e.Put(assignment(name, append(slices.Clone(endpoints), added)))
	})
}

func (m *mesh) removeEndpoint(name string, endpoint netip.AddrPort) {
	removed := lbEndpoint(endpoint)
	endpoints := m.endpoints(name)
	kept := slices.DeleteFunc(slices.Clone(endpoints), func(e *endpointv3.LbEndpoint) bool { return proto.Equal(e, removed) })
	if len(kept) == len(endpoints) {
		return
	}
	m.src.Update(func(c *snapshot.Change[string, int64]) {
		c.EditGroup(clusters, func(e *snapshot.Editor) { e.Put(assignment(name, kept)) })
	})
}

func (m *mesh) remove(name string) {
	// Removing the cluster drops it from every port's routes too: Envoy gets the new routes first.
	m.changeClusters(func(e *snapshot.Editor) {
		e.Remove(xds.ClusterType, name)
		e.Remove(xds.EndpointType, name)
	})
}

func (m *mesh) endpoints(name string) []*endpointv3.LbEndpoint {
	cla, ok := m.src.Group(clusters).Resource(xds.EndpointType, name)
	if !ok {
		return nil
	}
	return cla.(*endpointv3.ClusterLoadAssignment).GetEndpoints()[0].GetLbEndpoints()
}

func cluster(name string) *clusterv3.Cluster {
	return &clusterv3.Cluster{
		Name:                 name,
		ClusterDiscoveryType: &clusterv3.Cluster_Type{Type: clusterv3.Cluster_EDS},
		ConnectTimeout:       durationpb.New(time.Second),
		EdsClusterConfig: &clusterv3.Cluster_EdsClusterConfig{EdsConfig: &corev3.ConfigSource{
			ResourceApiVersion:    corev3.ApiVersion_V3,
			ConfigSourceSpecifier: &corev3.ConfigSource_Ads{Ads: &corev3.AggregatedConfigSource{}},
		}},
	}
}

func lbEndpoint(e netip.AddrPort) *endpointv3.LbEndpoint {
	return &endpointv3.LbEndpoint{HostIdentifier: &endpointv3.LbEndpoint_Endpoint{Endpoint: &endpointv3.Endpoint{
		Address: &corev3.Address{Address: &corev3.Address_SocketAddress{SocketAddress: &corev3.SocketAddress{
			Protocol:      corev3.SocketAddress_TCP,
			Address:       e.Addr().String(),
			PortSpecifier: &corev3.SocketAddress_PortValue{PortValue: uint32(e.Port())},
		}}},
	}}}
}

func assignment(name string, endpoints []*endpointv3.LbEndpoint) *endpointv3.ClusterLoadAssignment {
	return &endpointv3.ClusterLoadAssignment{ClusterName: name,
		Endpoints: []*endpointv3.LocalityLbEndpoints{{LbEndpoints: endpoints}}}
}

const help = `Commands:
  list - list all service names
  show <name> - show endpoints for service by name
  add <name> <ip:port> - add a new endpoint to a service, creating it if needed
  remove <name> <ip:port> - remove an endpoint from a service
  remove <name> - remove a service
  quit - shutdown`

func cli(m *mesh) {
	fmt.Println("CLI driven service mesh, snapshot edition!")
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

func command(m *mesh, args []string) error {
	switch {
	case args[0] == "list" && len(args) == 1:
		fmt.Println("Services:")
		for _, name := range names(m.src.Group(clusters), xds.ClusterType) {
			fmt.Printf("\t%s\n", name)
		}
	case args[0] == "show" && len(args) == 2:
		fmt.Println("Service:", args[1])
		if _, ok := m.src.Group(clusters).Resource(xds.ClusterType, args[1]); !ok {
			fmt.Println("\tNot Found")
		}
		for _, e := range m.endpoints(args[1]) {
			a := e.GetEndpoint().GetAddress().GetSocketAddress()
			fmt.Printf("\t%s\n", net.JoinHostPort(a.GetAddress(), strconv.Itoa(int(a.GetPortValue()))))
		}
	case args[0] == "add" && len(args) == 3:
		e, err := netip.ParseAddrPort(args[2])
		if err != nil {
			return err
		}
		m.add(args[1], e)
	case args[0] == "remove" && len(args) == 3:
		e, err := netip.ParseAddrPort(args[2])
		if err != nil {
			return err
		}
		m.removeEndpoint(args[1], e)
	case args[0] == "remove" && len(args) == 2:
		m.remove(args[1])
	case args[0] == "help" && len(args) == 1:
		fmt.Println(help)
	default:
		return errors.New("unknown command, or the wrong number of arguments")
	}
	return nil
}
