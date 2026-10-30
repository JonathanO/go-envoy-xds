// Package xds is an xDS control plane library for Envoy. It serves config to Envoys over ADS (the aggregated
// discovery service, every resource type on one gRPC stream), with either protocol: SotW (state of the world, where
// each response is every resource of a type) or delta (where each response is only what changed).
//
// It sends every change make before break: whatever a change adds is in place before anything refers to it, and
// nothing is removed while anything still refers to it. A change goes in two rounds, adds and then removes, one type
// at a time, and waits for Envoy to ACK each type before sending the next.
//
// The main types:
//
//   - Server serves the streams. Register it with discoveryv3.RegisterAggregatedDiscoveryServiceServer.
//   - Source tells each Envoy's stream about changes to its config, as state updates of a type you choose.
//   - ConfigBuilder turns those updates into the resources of one type, one builder per type.
//   - EnvoyConfig says how to treat one Envoy. ServerConfig.ConfigFor makes one from the node in its first request.
//   - Events, Metrics and ResyncAfterNack tell you what Envoys do, and recover from a NACK.
//
// Package snapshot provides a Source and ConfigBuilders, from named groups of resources. Most control planes need
// nothing else. Write your own when your state is better kept another way.
//
// When an Envoy rejects config (a NACK), its stream is sent no more changes. Call Client.Resync once the config is
// fixed, or use ResyncAfterNack.
//
// The guide is docs/USAGE.md, and how it works is docs/ARCHITECTURE.md, both at
// https://github.com/JonathanO/go-envoy-xds.
package xds
