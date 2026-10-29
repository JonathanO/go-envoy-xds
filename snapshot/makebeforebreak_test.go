package snapshot_test

import (
	"fmt"
	"maps"
	"math/rand/v2"
	"slices"
	"testing"
	"testing/synctest"

	endpointv3 "github.com/envoyproxy/go-control-plane/envoy/config/endpoint/v3"
	listenerv3 "github.com/envoyproxy/go-control-plane/envoy/config/listener/v3"
	routev3 "github.com/envoyproxy/go-control-plane/envoy/config/route/v3"
	hcmv3 "github.com/envoyproxy/go-control-plane/envoy/extensions/filters/network/http_connection_manager/v3"
	discoveryv3 "github.com/envoyproxy/go-control-plane/envoy/service/discovery/v3"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"

	xds "github.com/JonathanO/go-envoy-xds"
	"github.com/JonathanO/go-envoy-xds/snapshot"
)

// Make before break, against a model of what an Envoy holds. A delta or SotW stream runs on the real Server. Between
// random changes to the Source, the model applies our responses one at a time, as Envoy does: it ACKs each, then
// subscribes to the endpoints of the clusters it holds and the route configs of its listeners. After every response,
// what it serves from must refer only to config it holds, and once everything is delivered it holds the snapshot.
//
// The Envoy's view is one or two groups. Each snapshot is split across them at random, with decoys in the group that
// should lose, and published with the view's groups as one change: whole groups, or edits of the current ones, so
// only the names touched are compared. A group in another view also changes, which this Envoy must never see, so the
// updates it does get skip versions. Updates wait until the fuzzer delivers them, so several can merge on the way.

func FuzzEnvoysOnlyServeFromConfigTheyHold(f *testing.F) {
	r := rand.New(rand.NewPCG(1, 2))
	for range 200 {
		choices := make([]byte, r.IntN(200))
		for i := range choices {
			choices[i] = byte(r.Uint32())
		}
		f.Add(choices)
	}
	f.Fuzz(func(t *testing.T, choices []byte) {
		for _, delta := range []bool{true, false} {
			synctest.Test(t, func(t *testing.T) {
				m := newMachine(t, delta, &draws{choices})
				for len(m.d.left) > 0 {
					m.step()
					m.envoy.check()
				}
				m.settle()
				m.envoy.check()
			})
		}
	})
}

// draws are a fuzzer's choices, a byte each. Once they run out, every choice is the first.
type draws struct{ left []byte }

func (d *draws) intn(n int) int {
	if len(d.left) == 0 {
		return 0
	}
	c := int(d.left[0]) % n
	d.left = d.left[1:]
	return c
}

func (d *draws) bool() bool { return d.intn(2) == 1 }

func pick[T any](d *draws, from []T) T { return from[d.intn(len(from))] }

var views = [][]string{{"own", "shared"}, {"shared", "own"}, {"own"}, {"shared"}}

type machine struct {
	d         *draws
	src       *queued
	envoy     *model
	intended  *snapshot.Snapshot
	elsewhere int
}

func newMachine(t *testing.T, delta bool, d *draws) *machine {
	src := &queued{src: snapshot.New[string, string, string](func(string) string { return "envoy" })}
	srv := newServer(t, src, xds.ClusterType, xds.EndpointType, xds.ListenerType, xds.RouteType)
	e := &model{t: t, clusters: held{}, endpoints: held{}, listeners: held{}, routeConfigs: held{}, serving: map[string]string{},
		edsSubs: set{}, rdsSubs: set{}, edsAnswered: set{}, acked: map[string]received{}}
	if delta {
		e.delta = connect(t, srv.DeltaAggregatedResources)
	} else {
		e.sotw = connect(t, srv.StreamAggregatedResources)
	}
	e.connect()
	src.Update(func(c *snapshot.Change[string, string]) { c.SetView("elsewhere", "other") })
	return &machine{d: d, src: src, envoy: e, intended: snapshot.Empty()}
}

func (m *machine) step() {
	switch c := m.d.intn(10); {
	case c < 2:
		m.publish()
	case c < 3:
		m.src.Update(m.changeElsewhere)
	case c < 5:
		m.src.deliver()
		m.envoy.collect()
	case c < 9:
		if len(m.envoy.inbox) > 0 {
			m.envoy.deliver()
		}
	default:
		m.settle()
	}
}

func (m *machine) publish() {
	next := m.drawSnapshot()
	view := pick(m.d, views)
	groups := m.split(next, view)
	elsewhere := m.d.bool()
	m.src.Update(func(c *snapshot.Change[string, string]) {
		c.SetView("envoy", view...)
		for _, name := range []string{"own", "shared"} {
			if m.d.bool() {
				c.EditGroup(name, func(e *snapshot.Editor) { editTo(e, c.Group(name), groups[name]) })
			} else {
				c.SetGroup(name, groups[name])
			}
		}
		if elsewhere {
			m.changeElsewhere(c)
		}
	})
	m.intended = next
}

// changeElsewhere changes a group the Envoy's view doesn't have, whatever its groups.
func (m *machine) changeElsewhere(c *snapshot.Change[string, string]) {
	m.elsewhere++
	a := cluster("a", 1)
	a.AltStatName = fmt.Sprint("elsewhere-", m.elsewhere)
	c.EditGroup("other", func(e *snapshot.Editor) { e.Put(a) })
}

func (m *machine) settle() {
	m.src.deliverAll()
	m.envoy.collect()
	m.envoy.settle()
	m.envoy.holds(m.intended)
}

// drawSnapshot draws a consistent snapshot: every reference resolves.
func (m *machine) drawSnapshot() *snapshot.Snapshot {
	e := snapshot.Empty().Edit()
	var clusters []string
	for _, c := range []string{"a", "b", "c"} {
		if m.d.bool() {
			clusters = append(clusters, c)
			e.Put(cluster(c, 1))
			e.Put(cla(c, pick(m.d, []string{"10.0.0.1", "10.0.0.2"})))
		}
	}
	var routeConfigs []string
	for _, r := range []string{"r1", "r2"} {
		if m.d.bool() {
			var to []string
			for _, c := range clusters {
				if m.d.bool() {
					to = append(to, c)
				}
			}
			e.Put(routes(r, to...))
			routeConfigs = append(routeConfigs, r)
		}
	}
	for _, l := range []string{"l1", "l2"} {
		if len(routeConfigs) > 0 && m.d.bool() {
			e.Put(listener(l, pick(m.d, routeConfigs)))
		}
	}
	return e.Build()
}

// split splits a snapshot across the view's groups: each resource in one of them, or in the first with a decoy after.
func (m *machine) split(s *snapshot.Snapshot, view []string) map[string]*snapshot.Snapshot {
	groups := map[string]*snapshot.Editor{"own": snapshot.Empty().Edit(), "shared": snapshot.Empty().Edit()}
	hidden := ""
	if len(view) == 1 {
		hidden = map[string]string{"own": "shared", "shared": "own"}[view[0]]
	}
	for _, t := range slices.Sorted(s.Types()) {
		for _, name := range slices.Sorted(maps.Keys(maps.Collect(s.Resources(t)))) {
			r, _ := s.Resource(t, name)
			first := pick(m.d, view)
			groups[first].PutNamed(name, r)
			after := hidden
			if after == "" {
				after = view[1-slices.Index(view, first)]
			}
			if slices.Index(view, first) == 0 && m.d.bool() {
				groups[after].PutNamed(name, decoy(r))
			}
		}
	}
	built := map[string]*snapshot.Snapshot{}
	for name, e := range groups {
		built[name] = e.Build()
	}
	return built
}

// decoy is the same resource, but not equal to it: what Envoy holds if a group that should lose wins.
func decoy(r proto.Message) proto.Message {
	d := proto.Clone(r)
	d.ProtoReflect().SetUnknown(protowire.AppendVarint(protowire.AppendTag(nil, 99999, protowire.VarintType), 1))
	return d
}

// editTo edits current into next, so only the names that changed are compared.
func editTo(e *snapshot.Editor, current, next *snapshot.Snapshot) {
	for _, s := range []*snapshot.Snapshot{current, next} {
		for t := range s.Types() {
			for name := range current.Resources(t) {
				if _, ok := next.Resource(t, name); !ok {
					e.Remove(t, name)
				}
			}
			for name, r := range next.Resources(t) {
				e.PutNamed(name, r)
			}
		}
	}
}

type (
	set  = map[string]struct{}
	held = map[string]proto.Message
)

// received is a response of either protocol. SotW has no removals: for LDS and CDS, what it leaves out is removed.
type received struct {
	typ            string
	resources      map[string]*anypb.Any
	removed        []string
	version, nonce string
}

// model is what an Envoy holds, and the route config each listener serves from.
type model struct {
	t     *testing.T
	delta *fake[discoveryv3.DeltaDiscoveryRequest, discoveryv3.DeltaDiscoveryResponse]
	sotw  *fake[discoveryv3.DiscoveryRequest, discoveryv3.DiscoveryResponse]
	inbox []received

	clusters, endpoints, listeners, routeConfigs held
	// A listener whose route config hasn't arrived warms; until then Envoy keeps serving the one it had.
	serving          map[string]string
	edsSubs, rdsSubs set
	// Clusters that have had an EDS answer; until then they warm.
	edsAnswered set
	// Envoy pauses CDS while clusters warm (ClusterManagerImpl), so its ACK waits for their endpoints.
	unackedCds *received
	// SotW: the last response of each type ACKed, whose version and nonce later requests carry.
	acked    map[string]received
	problems []string
}

// connect subscribes to every listener and cluster, as Envoy's first requests do.
func (e *model) connect() {
	for _, t := range []xds.TypeURL{xds.ClusterType, xds.ListenerType} {
		if e.delta != nil {
			e.sendDelta(&discoveryv3.DeltaDiscoveryRequest{Node: node, TypeUrl: string(t)})
		} else {
			e.sendSotw(&discoveryv3.DiscoveryRequest{Node: node, TypeUrl: string(t)})
		}
	}
}

func (e *model) sendDelta(req *discoveryv3.DeltaDiscoveryRequest) {
	e.receiveDelta(e.delta.send(req))
}

func (e *model) sendSotw(req *discoveryv3.DiscoveryRequest) {
	e.receiveSotw(e.sotw.send(req))
}

// collect takes in what the server sent unasked, e.g. after an update.
func (e *model) collect() {
	if e.delta != nil {
		e.receiveDelta(e.delta.received())
	} else {
		e.receiveSotw(e.sotw.received())
	}
}

func (e *model) receiveDelta(rs []*discoveryv3.DeltaDiscoveryResponse) {
	for _, r := range rs {
		got := received{typ: r.GetTypeUrl(), resources: map[string]*anypb.Any{}, removed: r.GetRemovedResources(), nonce: r.GetNonce()}
		for _, res := range r.GetResources() {
			got.resources[res.GetName()] = res.GetResource()
		}
		e.inbox = append(e.inbox, got)
	}
}

func (e *model) receiveSotw(rs []*discoveryv3.DiscoveryResponse) {
	for _, r := range rs {
		got := received{typ: r.GetTypeUrl(), resources: map[string]*anypb.Any{}, version: r.GetVersionInfo(), nonce: r.GetNonce()}
		for _, a := range r.GetResources() {
			switch m := e.unpack(a).(type) {
			case *endpointv3.ClusterLoadAssignment:
				got.resources[m.GetClusterName()] = a
			case interface{ GetName() string }:
				got.resources[m.GetName()] = a
			}
		}
		e.inbox = append(e.inbox, got)
	}
}

// deliver applies the next response, ACKs it, then follows the new config's references with subscriptions.
func (e *model) deliver() {
	r := e.inbox[0]
	e.inbox = e.inbox[1:]
	e.apply(r)
	e.checkServing(r)
	if r.typ == string(xds.ClusterType) {
		e.unackedCds = &r
	} else {
		e.ack(r)
	}
	e.resubscribe(xds.EndpointType, e.edsSubs, keys(e.clusters), e.endpoints)
	maps.DeleteFunc(e.edsAnswered, func(c string, _ struct{}) bool { _, ok := e.clusters[c]; return !ok })
	if e.unackedCds != nil && len(e.edsAnswered) == len(e.clusters) {
		e.ack(*e.unackedCds)
		e.unackedCds = nil
	}
	wanted := set{}
	for _, rc := range e.serving {
		wanted[rc] = struct{}{}
	}
	for _, l := range e.listeners {
		wanted[routeConfigOf(l)] = struct{}{}
	}
	e.resubscribe(xds.RouteType, e.rdsSubs, wanted, e.routeConfigs)
}

func (e *model) ack(r received) {
	if e.delta != nil {
		e.sendDelta(&discoveryv3.DeltaDiscoveryRequest{TypeUrl: r.typ, ResponseNonce: r.nonce})
		return
	}
	e.acked[r.typ] = r
	e.sendSotw(e.sotwRequest(xds.TypeURL(r.typ)))
}

// sotwRequest lists every name subscribed to, and carries the last version ACKed.
func (e *model) sotwRequest(t xds.TypeURL) *discoveryv3.DiscoveryRequest {
	req := &discoveryv3.DiscoveryRequest{TypeUrl: string(t)}
	switch t {
	case xds.EndpointType:
		req.ResourceNames = slices.Sorted(maps.Keys(e.edsSubs))
	case xds.RouteType:
		req.ResourceNames = slices.Sorted(maps.Keys(e.rdsSubs))
	}
	if last, ok := e.acked[string(t)]; ok {
		req.VersionInfo, req.ResponseNonce = last.version, last.nonce
	}
	return req
}

func (e *model) settle() {
	for i := 0; i < 1000 && len(e.inbox) > 0; i++ {
		e.deliver()
	}
	if len(e.inbox) > 0 {
		e.t.Fatal("still answering after 1000 responses")
	}
}

func (e *model) apply(r received) {
	switch xds.TypeURL(r.typ) {
	case xds.ClusterType:
		e.update(r, e.clusters, nil, true)
	case xds.EndpointType:
		e.update(r, e.endpoints, e.edsSubs, true)
		for name := range r.resources {
			e.edsAnswered[name] = struct{}{}
		}
		for _, name := range r.removed {
			e.edsAnswered[name] = struct{}{}
		}
	case xds.ListenerType:
		e.update(r, e.listeners, nil, true)
		maps.DeleteFunc(e.serving, func(l, _ string) bool { _, ok := e.listeners[l]; return !ok })
	case xds.RouteType:
		// Envoy ignores delta RDS removals (RdsRouteConfigSubscription::onConfigUpdate): it drops a route config
		// when nothing uses it, by unsubscribing.
		e.update(r, e.routeConfigs, e.rdsSubs, false)
	default:
		e.problems = append(e.problems, "sent "+r.typ+", which Envoy never asked for")
	}
	// A warming listener starts serving once its route config arrives.
	for name, l := range e.listeners {
		if rc := routeConfigOf(l); e.routeConfigs[rc] != nil {
			e.serving[name] = rc
		}
	}
}

// update applies a response. For a type Envoy subscribes to by name, subscribed, it ignores names it no longer
// subscribes to. A SotW response for a wildcard type is the whole set.
func (e *model) update(r received, h held, subscribed set, honoursRemovals bool) {
	for name, a := range r.resources {
		if _, ok := subscribed[name]; ok || subscribed == nil {
			h[name] = e.unpack(a)
		}
	}
	if honoursRemovals {
		for _, name := range r.removed {
			delete(h, name)
		}
	}
	if e.sotw != nil && subscribed == nil {
		maps.DeleteFunc(h, func(name string, _ proto.Message) bool { _, ok := r.resources[name]; return !ok })
	}
}

func (e *model) checkServing(after received) {
	for l, rcName := range e.serving {
		rc := e.routeConfigs[rcName]
		if rc == nil {
			e.problem(after, "listener %s serves route config %s, which Envoy doesn't have", l, rcName)
			continue
		}
		for _, c := range clustersOf(rc.(*routev3.RouteConfiguration)) {
			switch {
			case e.clusters[c] == nil:
				e.problem(after, "route config %s routes to cluster %s, which Envoy doesn't have", rcName, c)
			case e.endpoints[c] == nil:
				// An EDS cluster is warming, and can't serve, until its endpoints arrive.
				e.problem(after, "route config %s routes to cluster %s, which has no endpoints", rcName, c)
			}
		}
	}
}

func (e *model) problem(after received, format string, args ...any) {
	e.problems = append(e.problems, fmt.Sprintf("after %s %v removing %v, ", after.typ, slices.Sorted(maps.Keys(after.resources)), after.removed)+
		fmt.Sprintf(format, args...))
}

func (e *model) check() {
	e.t.Helper()
	if len(e.problems) > 0 {
		e.t.Fatal(e.problems)
	}
}

func (e *model) resubscribe(t xds.TypeURL, subscribed, wanted set, h held) {
	var add, drop []string
	for name := range wanted {
		if _, ok := subscribed[name]; !ok {
			add = append(add, name)
		}
	}
	for name := range subscribed {
		if _, ok := wanted[name]; !ok {
			drop = append(drop, name)
		}
	}
	if len(add) == 0 && len(drop) == 0 {
		return
	}
	slices.Sort(add)
	slices.Sort(drop)
	for _, name := range add {
		subscribed[name] = struct{}{}
	}
	for _, name := range drop {
		delete(subscribed, name)
		delete(h, name) // Envoy forgets what it no longer subscribes to.
	}
	if e.delta != nil {
		e.sendDelta(&discoveryv3.DeltaDiscoveryRequest{TypeUrl: string(t), ResourceNamesSubscribe: add, ResourceNamesUnsubscribe: drop})
	} else {
		e.sendSotw(e.sotwRequest(t))
	}
}

// holds checks Envoy holds a snapshot: all of it, but the route configs no listener uses.
func (e *model) holds(s *snapshot.Snapshot) {
	e.t.Helper()
	used := set{}
	for _, l := range s.Resources(xds.ListenerType) {
		used[routeConfigOf(l)] = struct{}{}
	}
	routeConfigs := maps.Collect(s.Resources(xds.RouteType))
	maps.DeleteFunc(routeConfigs, func(name string, _ proto.Message) bool { _, ok := used[name]; return !ok })
	for _, c := range []struct {
		what       string
		held, want held
	}{
		{"clusters", e.clusters, maps.Collect(s.Resources(xds.ClusterType))},
		{"endpoints", e.endpoints, maps.Collect(s.Resources(xds.EndpointType))},
		{"listeners", e.listeners, maps.Collect(s.Resources(xds.ListenerType))},
		{"route configs", e.routeConfigs, routeConfigs},
	} {
		if !maps.EqualFunc(c.held, c.want, proto.Equal) {
			e.t.Fatalf("Envoy holds %s %v, want %v", c.what, c.held, c.want)
		}
	}
}

func (e *model) unpack(a *anypb.Any) proto.Message {
	m, err := a.UnmarshalNew()
	if err != nil {
		e.t.Fatal(err)
	}
	return m
}

func keys(h held) set {
	s := set{}
	for name := range h {
		s[name] = struct{}{}
	}
	return s
}

func routeConfigOf(l proto.Message) string {
	var hcm hcmv3.HttpConnectionManager
	if err := l.(*listenerv3.Listener).GetFilterChains()[0].GetFilters()[0].GetTypedConfig().UnmarshalTo(&hcm); err != nil {
		panic(err)
	}
	return hcm.GetRds().GetRouteConfigName()
}

func clustersOf(rc *routev3.RouteConfiguration) []string {
	var cs []string
	for _, vh := range rc.GetVirtualHosts() {
		for _, r := range vh.GetRoutes() {
			cs = append(cs, r.GetRoute().GetCluster())
		}
	}
	return cs
}
