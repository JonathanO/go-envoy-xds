// Package snapshot serves config from named groups of resources, in make-before-break order with no ordering code of
// your own. It provides an xds.Source, and the xds.ConfigBuilders to serve it.
//
// The main types:
//
//   - Snapshot is an immutable set of resources, of any types. Make one from another with Edit.
//   - Source holds groups, each a Snapshot, and views, each an ordered list of groups. Every Envoy sees one view,
//     picked from its details; where two groups have the same resource, the first group's wins.
//   - Change is what Source.Update edits: groups, views, and phases with Then.
//   - Update is what the Source sends each Envoy's stream: one view going from one state to the next.
//
// Publishing a change wakes only the Envoys whose view it touches, and each compares only the names you touched.
// Serve it with Builders:
//
//	src := snapshot.New[string, string, string](func(node string) string { return "all" })
//	src.Update(func(c *snapshot.Change[string, string]) {
//		c.SetView("all", "all")
//		c.EditGroup("all", func(e *snapshot.Editor) { e.Put(cluster) })
//	})
//	srv, err := xds.NewServer(xds.ServerConfig[*snapshot.Update, string]{
//		Source:   src,
//		Builders: snapshot.Builders[string](xds.ClusterType, xds.EndpointType, xds.ListenerType, xds.RouteType),
//		ConfigFor: func(_ context.Context, n *corev3.Node) (xds.EnvoyConfig[string], error) {
//			return xds.EnvoyConfig[string]{Details: n.GetId()}, nil
//		},
//	})
//
// The guide is docs/USAGE.md at https://github.com/JonathanO/go-envoy-xds.
package snapshot
