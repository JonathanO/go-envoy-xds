package xds

import (
	"slices"

	"google.golang.org/protobuf/proto"
)

// TypeURL names an xDS resource type, e.g. "type.googleapis.com/envoy.config.cluster.v3.Cluster".
type TypeURL string

// The resource types a Server can serve.
const (
	ListenerType    TypeURL = "type.googleapis.com/envoy.config.listener.v3.Listener"
	RouteType       TypeURL = "type.googleapis.com/envoy.config.route.v3.RouteConfiguration"
	ScopedRouteType TypeURL = "type.googleapis.com/envoy.config.route.v3.ScopedRouteConfiguration"
	VirtualHostType TypeURL = "type.googleapis.com/envoy.config.route.v3.VirtualHost"
	ClusterType     TypeURL = "type.googleapis.com/envoy.config.cluster.v3.Cluster"
	EndpointType    TypeURL = "type.googleapis.com/envoy.config.endpoint.v3.ClusterLoadAssignment"
	SecretType      TypeURL = "type.googleapis.com/envoy.extensions.transport_sockets.tls.v3.Secret"
	RuntimeType     TypeURL = "type.googleapis.com/envoy.service.runtime.v3.Runtime"
)

// Make before break: a change is sent in add order, then in remove order, so what a resource refers to arrives
// before it and leaves after it. Partly from the xDS spec, partly guesswork where the order probably doesn't matter.
var (
	addOrder    = []TypeURL{SecretType, RuntimeType, ClusterType, EndpointType, ListenerType, RouteType, ScopedRouteType, VirtualHostType}
	removeOrder = []TypeURL{ListenerType, ScopedRouteType, RouteType, VirtualHostType, ClusterType, EndpointType, RuntimeType, SecretType}
)

// Wildcard reports whether Envoy may subscribe to every resource of the type at once, as it does for listeners,
// clusters, scoped routes and virtual hosts.
func (t TypeURL) Wildcard() bool {
	switch t {
	case ListenerType, ClusterType, ScopedRouteType, VirtualHostType:
		return true
	}
	return false
}

func (t TypeURL) known() bool {
	return slices.Contains(addOrder, t)
}

// TypeOf returns the type URL of a resource, e.g. ClusterType for a *clusterv3.Cluster.
func TypeOf(m proto.Message) TypeURL {
	return TypeURL("type.googleapis.com/" + m.ProtoReflect().Descriptor().FullName())
}
