# Examples

Two toy control planes, driven from stdin. They're not for production: a real one wouldn't take commands on a CLI, and
would want metrics, and config that suits your setup.

Each serves ADS on port 6566. Add a service on the CLI, and Envoy gets a cluster for it, with its endpoints, and a
virtual host that routes requests for the service's name to it.

- [builders](builders) builds Envoy's config from state of its own: a `clusterManager` is its `xds.Source`, and it
  has an `xds.ConfigBuilder` per type.
- [snapshot](snapshot) does the same on package `snapshot`. The clusters and endpoints are one group, edited by each
  command. The route config depends on the port each Envoy listens on, so each port gets a group with its route
  config, and a view of that group and the clusters. The first Envoy to connect on a port makes them.

## Run one

From this directory, start an example:

```
go run ./builders
```

or `go run ./snapshot`. Then start Envoy with [envoy-delta.yaml](envoy-delta.yaml), which serves HTTP on port 10000:

```
envoy -c envoy-delta.yaml
```

At the example's prompt, add two endpoints to a service:

```
add foobar 1.1.1.1:80
add foobar 1.1.1.2:80
```

Envoy now has a cluster called foobar, balancing over those two, and routes requests for foobar on port 10000 to it:

```
curl --resolve foobar:10000:127.0.0.1 http://foobar:10000/
```

`help` lists the other commands: `list`, `show`, `remove` and `quit`.
