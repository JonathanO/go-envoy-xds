package main

import (
	"fmt"
	"maps"
	"net/netip"
	"slices"
	"time"

	clusterv3 "github.com/envoyproxy/go-control-plane/envoy/config/cluster/v3"
	corev3 "github.com/envoyproxy/go-control-plane/envoy/config/core/v3"
	endpointv3 "github.com/envoyproxy/go-control-plane/envoy/config/endpoint/v3"
	routev3 "github.com/envoyproxy/go-control-plane/envoy/config/route/v3"
	"google.golang.org/protobuf/types/known/durationpb"

	xds "github.com/JonathanO/go-envoy-xds"
)

// Each service makes one cluster and one ClusterLoadAssignment, named after it, so the builders can ask subscribed
// about the service's name. A real control plane may make several clusters from one service, and must take more care.

// routeName is the route config the http connection manager in envoy-delta.yaml asks for. It always exists.
const routeName = "all_routes"

// clusterBuilder makes a cluster per service. Nothing about a service changes its cluster, so clusters are only added
// and removed.
type clusterBuilder struct{}

func (clusterBuilder) Type() xds.TypeURL { return xds.ClusterType }

func (clusterBuilder) AddOrder(u update, subscribed func(string) bool, _ int64) xds.Response {
	var r xds.Response
	for name, s := range u.after {
		if _, ok := u.before[name]; !ok && subscribed(name) {
			r.Updates = append(r.Updates, cluster(s))
		}
	}
	return r
}

func (clusterBuilder) RemoveOrder(u update, subscribed func(string) bool, _ int64) xds.Response {
	var r xds.Response
	for name := range u.before {
		if _, ok := u.after[name]; !ok && subscribed(name) {
			r.Removes = append(r.Removes, name)
		}
	}
	return r
}

func (clusterBuilder) ResourcesAddOrder(u update, subscribed func(string) bool, _ int64) []xds.Resource {
	return clustersOf(u.preRemove(), subscribed)
}

func (clusterBuilder) ResourcesRemoveOrder(u update, subscribed func(string) bool, _ int64) []xds.Resource {
	return clustersOf(u.after, subscribed)
}

func clustersOf(services map[string]service, subscribed func(string) bool) []xds.Resource {
	var rs []xds.Resource
	for name, s := range services {
		if subscribed(name) {
			rs = append(rs, cluster(s))
		}
	}
	return rs
}

func cluster(s service) xds.Resource {
	return xds.Resource{Name: s.name, Message: &clusterv3.Cluster{
		Name:                 s.name,
		ClusterDiscoveryType: &clusterv3.Cluster_Type{Type: clusterv3.Cluster_EDS},
		ConnectTimeout:       durationpb.New(time.Second),
		EdsClusterConfig: &clusterv3.Cluster_EdsClusterConfig{EdsConfig: &corev3.ConfigSource{
			ResourceApiVersion:    corev3.ApiVersion_V3,
			ConfigSourceSpecifier: &corev3.ConfigSource_Ads{Ads: &corev3.AggregatedConfigSource{}},
		}},
	}}
}

// endpointBuilder makes a ClusterLoadAssignment per service. New endpoints go in the add round, alongside the old
// ones, and the old ones go in the remove round.
type endpointBuilder struct{}

func (endpointBuilder) Type() xds.TypeURL { return xds.EndpointType }

func (endpointBuilder) AddOrder(u update, subscribed func(string) bool, _ int64) xds.Response {
	var r xds.Response
	for name, after := range u.after {
		if !subscribed(name) {
			continue
		}
		before, ok := u.before[name]
		switch {
		case !ok:
			r.Updates = append(r.Updates, loadAssignment(after))
		case !containsAll(before.endpoints, after.endpoints):
			r.Updates = append(r.Updates, loadAssignment(combined(before, after)))
		}
	}
	return r
}

func (endpointBuilder) RemoveOrder(u update, subscribed func(string) bool, _ int64) xds.Response {
	var r xds.Response
	for name, before := range u.before {
		if !subscribed(name) {
			continue
		}
		after, ok := u.after[name]
		switch {
		case !ok:
			r.Removes = append(r.Removes, name)
		case !containsAll(after.endpoints, before.endpoints):
			// The add round sent the old endpoints too.
			r.Updates = append(r.Updates, loadAssignment(after))
		}
	}
	return r
}

// ResourcesAddOrder is what AddOrder sent: a service's old endpoints alongside its new ones. Only the new ones would
// drop the old ones early, e.g. when Envoy asks again for a cluster's endpoints after the change's CDS.
func (endpointBuilder) ResourcesAddOrder(u update, subscribed func(string) bool, _ int64) []xds.Resource {
	var rs []xds.Resource
	for name, s := range u.preRemove() {
		if subscribed(name) {
			if before, ok := u.before[name]; ok {
				s = combined(before, s)
			}
			rs = append(rs, loadAssignment(s))
		}
	}
	return rs
}

func (endpointBuilder) ResourcesRemoveOrder(u update, subscribed func(string) bool, _ int64) []xds.Resource {
	var rs []xds.Resource
	for name, s := range u.after {
		if subscribed(name) {
			rs = append(rs, loadAssignment(s))
		}
	}
	return rs
}

// combined is a service with its endpoints both before and after a change.
func combined(before, after service) service {
	endpoints := slices.Clone(after.endpoints)
	for _, e := range before.endpoints {
		if !slices.Contains(endpoints, e) {
			endpoints = append(endpoints, e)
		}
	}
	return service{after.name, endpoints}
}

func containsAll(have, want []netip.AddrPort) bool {
	for _, e := range want {
		if !slices.Contains(have, e) {
			return false
		}
	}
	return true
}

func loadAssignment(s service) xds.Resource {
	lbs := make([]*endpointv3.LbEndpoint, len(s.endpoints))
	for i, e := range s.endpoints {
		lbs[i] = &endpointv3.LbEndpoint{HostIdentifier: &endpointv3.LbEndpoint_Endpoint{Endpoint: &endpointv3.Endpoint{
			Address: &corev3.Address{Address: &corev3.Address_SocketAddress{SocketAddress: &corev3.SocketAddress{
				Protocol:      corev3.SocketAddress_TCP,
				Address:       e.Addr().String(),
				PortSpecifier: &corev3.SocketAddress_PortValue{PortValue: uint32(e.Port())},
			}}},
		}}}
	}
	return xds.Resource{Name: s.name, Message: &endpointv3.ClusterLoadAssignment{ClusterName: s.name,
		Endpoints: []*endpointv3.LocalityLbEndpoints{{LbEndpoints: lbs}}}}
}

// routeBuilder makes one route config, with a virtual host per service. It's one resource that names every cluster,
// so it changes whenever a service is added or removed, and is sent whole.
type routeBuilder struct{}

func (routeBuilder) Type() xds.TypeURL { return xds.RouteType }

// AddOrder routes to new services, alongside those yet to go.
func (routeBuilder) AddOrder(u update, subscribed func(string) bool, port int64) xds.Response {
	if !subscribed(routeName) || containsKeys(u.before, u.after) {
		return xds.Response{}
	}
	return xds.Response{Updates: []xds.Resource{routeConfig(u.preRemove(), port)}}
}

// RemoveOrder stops routing to removed services.
func (routeBuilder) RemoveOrder(u update, subscribed func(string) bool, port int64) xds.Response {
	if !subscribed(routeName) || containsKeys(u.after, u.before) {
		return xds.Response{}
	}
	return xds.Response{Updates: []xds.Resource{routeConfig(u.after, port)}}
}

func (routeBuilder) ResourcesAddOrder(u update, subscribed func(string) bool, port int64) []xds.Resource {
	if !subscribed(routeName) {
		return nil
	}
	return []xds.Resource{routeConfig(u.preRemove(), port)}
}

func (routeBuilder) ResourcesRemoveOrder(u update, subscribed func(string) bool, port int64) []xds.Resource {
	if !subscribed(routeName) {
		return nil
	}
	return []xds.Resource{routeConfig(u.after, port)}
}

// containsKeys reports whether have has every service want has.
func containsKeys(have, want map[string]service) bool {
	for name := range want {
		if _, ok := have[name]; !ok {
			return false
		}
	}
	return true
}

// routeConfig is the route config for Envoys listening on port. Its virtual hosts are sorted, so the same services make
// the same config.
func routeConfig(services map[string]service, port int64) xds.Resource {
	rc := &routev3.RouteConfiguration{Name: routeName}
	for _, name := range slices.Sorted(maps.Keys(services)) {
		rc.VirtualHosts = append(rc.VirtualHosts, &routev3.VirtualHost{
			Name:    name,
			Domains: []string{name, fmt.Sprintf("%s:%d", name, port)},
			Routes: []*routev3.Route{{
				Match:  &routev3.RouteMatch{PathSpecifier: &routev3.RouteMatch_Prefix{Prefix: "/"}},
				Action: &routev3.Route_Route{Route: &routev3.RouteAction{ClusterSpecifier: &routev3.RouteAction_Cluster{Cluster: name}}},
			}},
		})
	}
	return xds.Resource{Name: routeName, Message: rc}
}
