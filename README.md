# go-envoy-xds

An xDS control plane library for Envoy, in Go. It serves the config you give it to Envoys over ADS, SotW or delta, in
make-before-break order: whatever a change adds is in place before anything refers to it, and nothing is removed while
anything still refers to it. A Go port of [java-envoy-xds](https://github.com/JonathanO/java-envoy-xds).

It's for Envoy only, and aims to be the best at that.

## Use

Package `snapshot` holds your config as named groups of resources, and views made of groups: every Envoy sees one
view. Publishing a change wakes only the Envoys whose view it touches, and each compares only the names you touched.

```go
src := snapshot.New[string, string, string](func(nodeID string) string { return "all" })
src.Update(func(c *snapshot.Change[string, string]) {
	c.SetView("all", "routes", "clusters")
	c.EditGroup("clusters", func(e *snapshot.Editor) { e.Put(cluster); e.Put(endpoints) })
	c.EditGroup("routes", func(e *snapshot.Editor) { e.Put(listener); e.Put(routeConfig) })
})

srv, err := xds.NewServer(xds.ServerConfig[*snapshot.Update, string]{
	Source:   src,
	Builders: snapshot.Builders[string](xds.ClusterType, xds.EndpointType, xds.ListenerType, xds.RouteType),
	ConfigFor: func(_ context.Context, node *corev3.Node) (xds.EnvoyConfig[string], error) {
		return xds.EnvoyConfig[string]{Details: node.GetId()}, nil
	},
})
if err != nil {
	return err
}
discoveryv3.RegisterAggregatedDiscoveryServiceServer(grpcServer, srv)
```

To serve config from your own state instead, implement `xds.Source`, and an `xds.ConfigBuilder` per type.

## How it serves Envoy

- **Make before break.** Each change goes in two rounds, adds then removes, a type at a time, waiting for Envoy's ACK
  before the next. Clusters are added before the routes that use them, and removed after.
- **Bursts merge.** Updates that arrive while an Envoy is busy merge into one, if their type is an `xds.Merger`, as
  `snapshot.Update` is. Mesh updates come in bursts, and pushing each to every Envoy is expensive.
- **Phases.** To order changes make before break can't see, split an update with `Change.Then`: each Envoy gets, and
  ACKs, each phase before the next.
- **A NACK freezes the stream.** A rejected change won't fix itself, so the Envoy is sent no more changes and keeps its
  last good config. `Events.Nacked` tells you; `Client.Resync`, or `xds.ResyncAfterNack`, starts it afresh.
- **Reconnects.** What a delta Envoy kept over a reconnect, and is gone since, is removed in remove order, once the
  types that may refer to it are ACKed.

## Testing

`go test ./...` runs the unit tests. The `e2e` module runs the conformance suite from
[envoy-xds-testing-go](https://github.com/JonathanO/envoy-xds-testing-go) against real Envoys, 1.36 to 1.40, over both
protocols. It needs Docker: `cd e2e && go test ./...`, with `ENVOY_VERSIONS=1.40` to pick versions.
