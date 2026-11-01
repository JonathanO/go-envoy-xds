# Using go-envoy-xds

This guide is for building a control plane with go-envoy-xds. [ARCHITECTURE.md](ARCHITECTURE.md) is for changing
the library itself, and the API reference is on [pkg.go.dev](https://pkg.go.dev/github.com/JonathanO/go-envoy-xds).

A few terms first. Envoy's config is made of *resources*: listeners, routes, clusters, endpoints and so on, each a
protobuf message with a name and a *type*. Envoy fetches them from a control plane over xDS. This library serves
*ADS*, where one gRPC stream carries every type, with either protocol: *SotW* (state of the world), where each
response is every resource of a type, or *delta*, where each response is only what changed. Envoy answers each
response with an *ACK* (accepted) or a *NACK* (rejected).

The library sends every change *make before break*: whatever a change adds is in place before anything refers to it,
and nothing is removed while anything still refers to it. You describe the config you want; it works out the order.

## 1. Install

```sh
go get github.com/JonathanO/go-envoy-xds
```

It needs Go 1.26 or later. The protos are go-control-plane's, `github.com/envoyproxy/go-control-plane/envoy`.

## 2. Quick start

Package `snapshot` holds your config as named *groups* of resources, and *views*, each an ordered list of groups.
Every Envoy sees one view. Here every Envoy sees the view `default`, made of one group, `backends`:

```go
package main

import (
	"context"
	"log"
	"net"

	clusterv3 "github.com/envoyproxy/go-control-plane/envoy/config/cluster/v3"
	corev3 "github.com/envoyproxy/go-control-plane/envoy/config/core/v3"
	endpointv3 "github.com/envoyproxy/go-control-plane/envoy/config/endpoint/v3"
	discoveryv3 "github.com/envoyproxy/go-control-plane/envoy/service/discovery/v3"
	"google.golang.org/grpc"

	xds "github.com/JonathanO/go-envoy-xds"
	"github.com/JonathanO/go-envoy-xds/snapshot"
)

func main() {
	src := snapshot.New[string, string, string](func(nodeID string) string { return "default" })
	src.Update(func(c *snapshot.Change[string, string]) {
		c.SetView("default", "backends")
		c.EditGroup("backends", func(e *snapshot.Editor) {
			e.Put(edsCluster("backend"))
			e.Put(endpoints("backend", "10.0.0.1", 8080))
		})
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
	lis, err := net.Listen("tcp", ":18000")
	if err != nil {
		log.Fatal(err)
	}
	log.Fatal(grpcServer.Serve(lis))
}

// edsCluster is a cluster whose endpoints come over ADS.
func edsCluster(name string) *clusterv3.Cluster {
	return &clusterv3.Cluster{
		Name:                 name,
		ClusterDiscoveryType: &clusterv3.Cluster_Type{Type: clusterv3.Cluster_EDS},
		EdsClusterConfig: &clusterv3.Cluster_EdsClusterConfig{EdsConfig: &corev3.ConfigSource{
			ResourceApiVersion:    corev3.ApiVersion_V3,
			ConfigSourceSpecifier: &corev3.ConfigSource_Ads{Ads: &corev3.AggregatedConfigSource{}},
		}},
	}
}

func endpoints(cluster, address string, port uint32) *endpointv3.ClusterLoadAssignment {
	return &endpointv3.ClusterLoadAssignment{
		ClusterName: cluster,
		Endpoints: []*endpointv3.LocalityLbEndpoints{{LbEndpoints: []*endpointv3.LbEndpoint{{
			HostIdentifier: &endpointv3.LbEndpoint_Endpoint{Endpoint: &endpointv3.Endpoint{
				Address: &corev3.Address{Address: &corev3.Address_SocketAddress{SocketAddress: &corev3.SocketAddress{
					Address:       address,
					PortSpecifier: &corev3.SocketAddress_PortValue{PortValue: port},
				}}},
			}},
		}}}},
	}
}
```

Listeners and routes go in the same way, with `e.Put`. `Put` names a resource by its `name` field, or `cluster_name`
for a ClusterLoadAssignment, as Envoy does; `PutNamed` takes a name of your own.

Point Envoy at it with a bootstrap like this, with `api_type: DELTA_GRPC` for delta:

```yaml
node:
  id: envoy-1
  cluster: demo
dynamic_resources:
  ads_config:
    api_type: GRPC
    transport_api_version: V3
    grpc_services:
      - envoy_grpc:
          cluster_name: ads_cluster
  cds_config:
    ads: {}
    resource_api_version: V3
  lds_config:
    ads: {}
    resource_api_version: V3
static_resources:
  clusters:
    - name: ads_cluster
      type: STRICT_DNS
      http2_protocol_options: {}
      load_assignment:
        cluster_name: ads_cluster
        endpoints:
          - lb_endpoints:
              - endpoint:
                  address:
                    socket_address: { address: 127.0.0.1, port_value: 18000 }
```

To change the config, call `Update` again, from any goroutine. Everything in one `Update` reaches each Envoy as one
change, in make-before-break order, whatever groups the resources are in:

```go
src.Update(func(c *snapshot.Change[string, string]) {
	c.EditGroup("backends", func(e *snapshot.Editor) {
		e.Put(endpoints("backend", "10.0.0.2", 8080))
	})
})
```

Only the Envoys whose view has a group you changed are woken, and each compares only the names you touched.
`EditGroup` edits a group in place. `SetGroup` replaces it with a `Snapshot` you built, and compares every name in
it, so prefer `EditGroup` for small changes to big groups. `RemoveGroup` removes one.

`edit` runs with the Source locked: don't call the Source from inside it.

## 3. Per-Envoy config

`ServerConfig.ConfigFor` is called once per stream, with the node from Envoy's first request. It returns the
`EnvoyConfig` for that stream. An error ends the stream, and Envoy retries. An error with a gRPC status, from
`status.Error` say, goes to Envoy as it is, so `codes.InvalidArgument` can reject a node, as below. Any other error is
logged as a warning, and Envoy gets `codes.Internal` with the message "xds: internal error", as it does for a panic.

`EnvoyConfig.Details` is a value of any type you choose, the `D` type parameter. The Server passes it to
`Source.Subscribe` and to every ConfigBuilder call for that Envoy. Package snapshot passes it to the `viewOf` func
you give `snapshot.New`, which picks the Envoy's view.

Say each Envoy names its team in its node metadata, and each team has a view. Group and view names can be types of
your own, so one can't be passed as the other by mistake:

```go
type details struct{ Team string }

type group string
type view string

src := snapshot.New[details, group, view](func(d details) view { return view(d.Team) })
src.Update(func(c *snapshot.Change[group, view]) {
	c.EditGroup("shared", func(e *snapshot.Editor) { e.Put(edsCluster("auth")) })
	c.EditGroup("payments", func(e *snapshot.Editor) { e.Put(edsCluster("ledger")) })
	// The first group wins where two have the same resource, so a team's group can override what's shared.
	c.SetView("payments", "payments", "shared")
})

srv, err := xds.NewServer(xds.ServerConfig[*snapshot.Update, details]{
	Source:   src,
	Builders: snapshot.Builders[details](xds.ClusterType, xds.EndpointType, xds.ListenerType, xds.RouteType),
	ConfigFor: func(_ context.Context, node *corev3.Node) (xds.EnvoyConfig[details], error) {
		team := node.GetMetadata().GetFields()["team"].GetStringValue()
		if team == "" {
			return xds.EnvoyConfig[details]{}, status.Error(codes.InvalidArgument, "node metadata has no team")
		}
		return xds.EnvoyConfig[details]{Details: details{Team: team}}, nil
	},
})
```

An Envoy whose view doesn't exist sees nothing until you add it. The view is picked when the stream starts, so moving
an Envoy to another view takes a new stream: change what `viewOf` returns for it, and call `Client.Resync`
([section 7](#7-nacks-and-resync)). Changing a view's groups with `SetView` reaches its Envoys straight away.

## 4. Your own Source and ConfigBuilders

Package snapshot fits most control planes. Write your own Source and ConfigBuilders when your state is better kept
another way: when you already hold it in your own model, say, or each Envoy's config is computed from a few inputs
rather than stored.

The update type, `U`, is yours. A Server never looks inside it: it hands each update to every ConfigBuilder. An
update usually needs both the state before and after, so the builders can tell what changed.

**A Source** has one method:

```go
Subscribe(details D, deliver func(update U)) (current U, cancel func())
```

- It returns the state now, which the stream's first answers come from, and starts delivering updates that follow on
  from it. It must not miss an update published while it runs.
- It calls `deliver` with each update, in order, one at a time. `deliver` never blocks, so you can call it holding
  your own lock, which is the easy way to keep the order.
- After `cancel` returns, `deliver` may still be called, and does nothing. The Server calls `cancel` when the stream
  ends, and when Envoy NACKs.
- Each update is pushed, ConfigBuilders and all, even if it's equal to the last. Skip those yourself if they're
  common.

**A ConfigBuilder** serves one type. Each change goes out in two rounds, every type's add round and then every type's
remove round, so return what an update adds or changes from `AddOrder`, and what it removes from `RemoveOrder`:

```go
type ConfigBuilder[U, D any] interface {
	Type() TypeURL
	AddOrder(update U, subscribed func(name string) bool, details D) Response
	RemoveOrder(update U, subscribed func(name string) bool, details D) Response
	ResourcesAddOrder(update U, subscribed func(name string) bool, details D) []Resource
	ResourcesRemoveOrder(update U, subscribed func(name string) bool, details D) []Resource
}
```

- `ResourcesAddOrder` is every resource mid-change: what the update adds, and what it's yet to remove.
  `ResourcesRemoveOrder` is every resource once it's done. Subscriptions are answered from them: from
  `ResourcesAddOrder` until the type's remove round has been sent, and after a NACK stops it, and otherwise from
  `ResourcesRemoveOrder`.
- Return only names `subscribed` reports true for. Envoy rejects resources it didn't ask for, and the Server logs a
  warning for them.
- Returning only what changed is best; returning more is safe, just more work.
- Give a ConfigBuilder for each type you serve, at most one per type. Requests for types with none are answered with
  nothing.

[`example_source_test.go`](../example_source_test.go) is a whole, runnable Source and ConfigBuilder for clusters,
with an Envoy played over delta.

**Merging.** If your update type implements `xds.Merger`, updates that arrive while an Envoy is busy merge into one:

```go
func (u *myUpdate) Merge(next *myUpdate) *myUpdate // the update equal to applying u, then next
```

Without it they queue, and each is pushed in turn, with its own round trips. To stop later updates merging into one,
for phases, give it a `MergesWithLater() bool` method that returns false for it.

## 5. EnvoyConfig options

```go
return xds.EnvoyConfig[string]{
	Details:                node.GetId(),
	SubscribesTo:           []xds.TypeURL{xds.ClusterType, xds.EndpointType, xds.ListenerType, xds.RouteType},
	DelayUpdatesUntilAckOf: xds.RouteType,
}, nil
```

- **`SubscribesTo`** is the types this Envoy gets from you, or nil for every type you serve. When a delta Envoy
  reconnects, it says what it kept, and what's gone since must be removed. A cluster is only removed once the types
  that may refer to it (listeners, routes, and so on) have been answered and ACKed. Leave out types the Envoy doesn't
  use, e.g. RouteType for an Envoy with static routes, or those removals wait for them, for 30 seconds or more.
- **`DelayUpdatesUntilAckOf`** holds changes until this Envoy first ACKs that type. RouteType is the usual one: Envoy
  ACKs routes only once its clusters have initialised, and endpoint changes meanwhile get in the way of that
  ([envoy#16035](https://github.com/envoyproxy/envoy/issues/16035)). Only set it to a type the Envoy subscribes to.
  If `SubscribesTo` leaves the type out, it's ignored, with a warning. With no ACK after 30 seconds, changes go
  anyway.

Both 30-second waits are checked only when something happens on the stream, a request or an update, not on a timer,
so either can last longer.

## 6. Phases

Make before break orders types: clusters go before the routes that use them, and are removed after. It can't see
when a change to an existing resource must wait for a change to a resource of a later type. Say a route stops using a subset of a cluster, and the cluster
drops the subset: both are changes, so both go in the add round, clusters first, and the route briefly uses a subset
that's gone.

`Change.Then` splits an update into phases. Each Envoy is sent everything before it, and ACKs it, before it's sent
anything after:

```go
src.Update(func(c *snapshot.Change[string, string]) {
	c.EditGroup("routes", func(e *snapshot.Editor) { e.Put(routeWithoutSubset) })
	c.Then()
	c.EditGroup("clusters", func(e *snapshot.Editor) { e.Put(clusterWithoutSubset) })
})
```

Each phase is still make before break within itself. Only Envoys whose view changes in a later phase wait for the
earlier one. An Envoy connecting meanwhile gets the end result.

## 7. NACKs and resync

When an Envoy rejects config, its stream *freezes*: it's sent no more changes. Envoy keeps serving its last good
config, which goes stale. A rejected change won't fix itself, and Envoy may have taken part of it, so carrying on
from a guess would make things worse. A frozen stream still answers new subscriptions.

`Events.Nacked` tells you. Fix the config, then call `Client.Resync`: it closes the stream, and Envoy reconnects and
syncs afresh. `ResyncAfterNack` does that for you, on a backoff: it waits `initial` after an
Envoy's first NACK, doubles the wait each time the Envoy NACKs again after a resync, up to `maxWait`, and starts over
once an Envoy goes twice `maxWait` without a NACK:

```go
resync := xds.NewResyncAfterNack[string](time.Minute, 30*time.Minute)
events := xds.Events[string]{
	Nacked: func(c *xds.Client[string], t xds.TypeURL, detail *statuspb.Status) {
		slog.Error("Envoy rejected config", "node", c.Node.GetId(), "type", t, "error", detail.GetMessage())
		resync.Nacked(c, t, detail)
	},
	Disconnected: resync.Disconnected,
}
```

While it waits, that Envoy gets no changes, so `maxWait` is the longest it serves stale config. Alert on NACKs
either way: they mean your config is wrong.

## 8. Events, Metrics and logging

**Events** (`ServerConfig.Events`) are told what Envoys do. Any may be nil.

- `Connected(c)`: an Envoy's first request arrived, and ConfigFor accepted it. `c.Node` is its node, `c.Config` its
  EnvoyConfig.
- `Disconnected(c)`: the stream of a Connected Envoy ended.
- `Nacked(c, type, detail)`: an Envoy rejected config. Called for every NACK.

Keep the `*Client` from `Connected` to resync an Envoy later. Each stream has its own Client.

**Metrics** (`ServerConfig.Metrics`) is a func that makes a `Metrics` for each stream. A stream's Metrics is called
only from that stream's goroutine, so fields of its own need no lock; anything shared between streams does:

```go
var pushesUnderWay, frozen atomic.Int64

type streamMetrics struct {
	began  time.Time
	frozen bool
}

func (m *streamMetrics) PushBegan() { m.began = time.Now(); pushesUnderWay.Add(1) }

func (m *streamMetrics) PushCompleted() {
	pushesUnderWay.Add(-1)
	slog.Debug("Push done", "took", time.Since(m.began))
	m.began = time.Time{}
}

func (m *streamMetrics) AwaitingAck() {}
func (m *streamMetrics) Acked()       {}
func (m *streamMetrics) Frozen()      { frozen.Add(1); m.frozen = true }

func (m *streamMetrics) Closed() {
	if !m.began.IsZero() { // ended mid-push
		pushesUnderWay.Add(-1)
	}
	if m.frozen {
		frozen.Add(-1)
	}
}
```

with `Metrics: func() xds.Metrics { return &streamMetrics{} }`.

- `PushBegan` and `PushCompleted` bracket each change: completed once Envoy ACKs the last of it, or a NACK stops it.
  A stream that ends mid-push gets `Closed` without `PushCompleted`.
- `AwaitingAck` and `Acked` bracket answers to subscriptions, and removals after a reconnect.
- `Frozen`: Envoy NACKed, so the stream gets no more changes until it ends. Alert on the gauge: a frozen Envoy
  serves its last good config until it reconnects.
- A stream with no push under way, and every `AwaitingAck` matched by an `Acked`, is idle.

**Logging** goes to `ServerConfig.Logger`, a `*slog.Logger`, or `slog.Default()` if nil. NACKs, requests for unknown
types, builders returning names Envoy didn't ask for, and giving up on a 30-second wait are warnings; panics are
errors. Every record of a stream carries the Envoy's node ID as `node`.

## 9. Concurrency rules for your code

Each Envoy stream has its own goroutine, which makes every call into your code for that stream. So:

- **ConfigFor, Events and Metrics** run on the stream's goroutine, which waits for them. Don't block in them for long.
  ConfigFor gets the stream's context, which ends with the stream.
- **ConfigBuilders** run on every stream's goroutine at once, never twice at once for one stream. The updates you
  hand them are shared between streams, so treat them as read-only.
- **Your Source's `deliver`** runs on whatever goroutine you call it from, and never blocks. Package snapshot calls it
  from `Update`, with the Source locked.
- **`Source.Subscribe` and its `cancel`** are called from the stream's goroutine, never from inside `deliver`.
- **`snapshot.Source.Update`** may be called from any goroutine. Its `edit` func runs with the Source locked, so it
  mustn't call the Source.
- **`Client.Resync`** may be called from any goroutine, at any time.
- A **panic** in ConfigFor, an event, a builder or Subscribe is logged, and ends only that stream, with
  `codes.Internal`.

## 10. Testing against a real Envoy

Unit tests can't tell you what Envoy does with your config. [envoy-xds-testing-go](https://github.com/JonathanO/envoy-xds-testing-go)
runs a real Envoy in Docker, records each xDS stream, and checks it: everything was ACKed, nothing was sent that
wasn't asked for, every change was make before break, and Envoy is healthy, routing where you expect.

```sh
go get github.com/JonathanO/envoy-xds-testing-go
```

A test serves your control plane through its `Recorder`, and starts an Envoy pointed at it, with a bootstrap as its
README describes:

```go
rec := &xdstest.Recorder{}
grpcServer := grpc.NewServer(grpc.StreamInterceptor(rec.Intercept))
discoveryv3.RegisterAggregatedDiscoveryServiceServer(grpcServer, srv)
lis, err := net.Listen("tcp", ":0")
if err != nil {
	t.Fatal(err)
}
go grpcServer.Serve(lis)
t.Cleanup(grpcServer.Stop)

envoy, err := xdstest.StartEnvoy(ctx, xdstest.EnvoyConfig{
	Image: "envoyproxy/envoy:v1.40-latest", Bootstrap: bootstrap, ADSPort: lis.Addr().(*net.TCPAddr).Port,
	ListenerPorts: []int{10000},
})
if err != nil {
	t.Fatal(err)
}
t.Cleanup(func() { envoy.Terminate(context.Background()) })
```

Its [README](https://github.com/JonathanO/envoy-xds-testing-go#testing-your-control-plane) has the whole test:
waiting for the initial sync, making a change, and checking it with a `Checkpoint`. If your Metrics can tell when a
stream is idle, as in [section 8](#8-events-metrics-and-logging), wait with `xdstest.SettledIdle`.

This library's own conformance suite against real Envoys is the one every envoy-xds port runs, in
[envoy-xds-spec](https://github.com/JonathanO/envoy-xds-spec)'s `conformance/`. It drives a conformance server, which
for this library is [e2e/](../e2e): a snapshot control plane, with a small HTTP API for the suite to change its config.

## 11. Examples

[`examples/`](../examples) has runnable control planes: [`examples/snapshot`](../examples/snapshot) with package
snapshot, and [`examples/builders`](../examples/builders) with a Source and ConfigBuilders of its own. Its
[README](../examples/README.md) says how to run them against Envoy.

The godoc examples run under `go test`:
[`ExampleNewServer`](https://pkg.go.dev/github.com/JonathanO/go-envoy-xds#example-NewServer),
[a custom Source](https://pkg.go.dev/github.com/JonathanO/go-envoy-xds#example-package-CustomSource),
[groups and views](https://pkg.go.dev/github.com/JonathanO/go-envoy-xds/snapshot#example-Source.Update) and
[phases](https://pkg.go.dev/github.com/JonathanO/go-envoy-xds/snapshot#example-Change.Then).
