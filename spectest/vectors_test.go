// Package spectest runs the trace vectors from envoy-xds-spec, the submodule at spec/, through the real Server and
// package snapshot. The format is in spec/docs/VECTORS.md.
package spectest

import (
	"bytes"
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"testing/synctest"

	clusterv3 "github.com/envoyproxy/go-control-plane/envoy/config/cluster/v3"
	corev3 "github.com/envoyproxy/go-control-plane/envoy/config/core/v3"
	endpointv3 "github.com/envoyproxy/go-control-plane/envoy/config/endpoint/v3"
	listenerv3 "github.com/envoyproxy/go-control-plane/envoy/config/listener/v3"
	routev3 "github.com/envoyproxy/go-control-plane/envoy/config/route/v3"
	tlsv3 "github.com/envoyproxy/go-control-plane/envoy/extensions/transport_sockets/tls/v3"
	discoveryv3 "github.com/envoyproxy/go-control-plane/envoy/service/discovery/v3"
	runtimev3 "github.com/envoyproxy/go-control-plane/envoy/service/runtime/v3"
	statuspb "google.golang.org/genproto/googleapis/rpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	xds "github.com/JonathanO/go-envoy-xds"
	"github.com/JonathanO/go-envoy-xds/snapshot"
)

const vectors = "../spec/vectors"

func TestVectors(t *testing.T) {
	files, err := filepath.Glob(filepath.Join(vectors, "*.json"))
	if err != nil {
		t.Fatal(err)
	}
	if len(files) == 0 {
		t.Fatalf("no vectors in %s: is the spec submodule checked out? git submodule update --init", vectors)
	}
	for _, file := range files {
		t.Run(strings.TrimSuffix(filepath.Base(file), ".json"), func(t *testing.T) {
			v := read(t, file)
			for _, d := range v.Differs {
				if d.Port == "go" {
					t.Skipf("Go differs: %s", d.Reason)
				}
			}
			for _, protocol := range v.Protocols {
				t.Run(protocol, func(t *testing.T) {
					synctest.Test(t, func(t *testing.T) { runVector(t, v, protocol) })
				})
			}
		})
	}
}

// The vector format: see spec/docs/VECTORS.md. Fields match the JSON's names, ignoring case.

type vector struct {
	Name, Description, Comment string
	Protocols                  []string
	Differs                    []struct{ Port, Reason string }
	Serves                     []string
	Envoys                     []struct {
		Name, View             string
		SubscribesTo           []string
		DelayUpdatesUntilAckOf string
		Comment                string
	}
	Steps []step
}

// A step is an update, a request or a disconnect. Its tags are for showing it in a failure.
type step struct {
	Update      []json.RawMessage `json:"update,omitempty"` // each an op, or "then"
	Request     string            `json:"request,omitempty"`
	Envoy       string            `json:"envoy,omitempty"`
	Subscribe   []string          `json:"subscribe,omitempty"`
	Unsubscribe []string          `json:"unsubscribe,omitempty"`
	Ack         string            `json:"ack,omitempty"`
	Nack        string            `json:"nack,omitempty"`
	Kept        []string          `json:"kept,omitempty"`
	Disconnect  string            `json:"disconnect,omitempty"`
	Expect      []response        `json:"expect,omitempty"`
	Comment     string            `json:"comment,omitempty"`
}

type op struct {
	SetView     string
	Groups      []string
	EditGroup   string
	Put         map[string][]json.RawMessage
	Remove      map[string][]string
	SetGroup    string
	Resources   map[string][]json.RawMessage
	RemoveGroup string
	Comment     string
}

type response struct {
	Type, Envoy, As string
	contents
	Sotw, Delta *contents
	Comment     string
}

// contents' fields are nil when the JSON leaves them out, so an override replaces only the fields it has.
type contents struct {
	Resources []json.RawMessage
	Removed   []string
}

func read(t *testing.T, file string) *vector {
	t.Helper()
	data, err := os.ReadFile(file)
	if err != nil {
		t.Fatal(err)
	}
	var v vector
	if err := strict(data, &v); err != nil {
		t.Fatalf("%s: %v", file, err)
	}
	if want := strings.TrimSuffix(filepath.Base(file), ".json"); v.Name != want {
		t.Fatalf("%s: vector %s should be in %s.json", file, v.Name, v.Name)
	}
	return &v
}

// strict decodes JSON, failing on fields the format doesn't have, such as a kind of step this runner doesn't know.
func strict(data []byte, v any) error {
	d := json.NewDecoder(bytes.NewReader(data))
	d.DisallowUnknownFields()
	return d.Decode(v)
}

// messages are an empty message of each type, by the name vectors give it.
var messages = map[string]proto.Message{
	"LDS":  &listenerv3.Listener{},
	"RDS":  &routev3.RouteConfiguration{},
	"SRDS": &routev3.ScopedRouteConfiguration{},
	"VHDS": &routev3.VirtualHost{},
	"CDS":  &clusterv3.Cluster{},
	"EDS":  &endpointv3.ClusterLoadAssignment{},
	"SDS":  &tlsv3.Secret{},
	"RTDS": &runtimev3.Runtime{},
}

func typeURL(name string) (xds.TypeURL, error) {
	m, ok := messages[name]
	if !ok {
		return "", fmt.Errorf("unknown type %q", name)
	}
	return xds.TypeOf(m), nil
}

func typeName(t xds.TypeURL) string {
	for name, m := range messages {
		if xds.TypeOf(m) == t {
			return name
		}
	}
	return string(t)
}

func parse(typ string, resources []json.RawMessage) ([]proto.Message, error) {
	m, ok := messages[typ]
	if !ok {
		return nil, fmt.Errorf("unknown type %q", typ)
	}
	var ms []proto.Message
	for _, raw := range resources {
		r := m.ProtoReflect().New().Interface()
		if err := protojson.Unmarshal(raw, r); err != nil {
			return nil, fmt.Errorf("not a %s resource: %s: %w", typ, raw, err)
		}
		ms = append(ms, r)
	}
	return ms, nil
}

func parseAll(byType map[string][]json.RawMessage) ([]proto.Message, error) {
	var all []proto.Message
	for typ, resources := range byType {
		ms, err := parse(typ, resources)
		if err != nil {
			return nil, err
		}
		all = append(all, ms...)
	}
	return all, nil
}

// nameOf is the name Envoy knows a resource by.
func nameOf(m proto.Message) string {
	if c, ok := m.(*endpointv3.ClusterLoadAssignment); ok {
		return c.GetClusterName()
	}
	return m.(interface{ GetName() string }).GetName()
}

// run is one vector on one protocol.
type run struct {
	delta     bool
	replaying bool // as spec/docs/REPLAY.md says: an ACK or NACK of "last" with no such response is left out
	ctx       context.Context
	src       *snapshot.Source[string, string, string]
	srv       *xds.Server[*snapshot.Update, string]
	envoys    []*envoy
	labels    map[string]*received
}

func runVector(t *testing.T, v *vector, protocol string) {
	r, err := newRun(t.Context(), v, protocol)
	if err != nil {
		t.Fatal(err)
	}
	for i, s := range v.Steps {
		shown := s
		shown.Expect = nil
		js, _ := json.Marshal(shown)
		where := fmt.Sprintf("%s [%s], step %d: %s", v.Name, protocol, i+1, js)
		if err := r.perform(s); err != nil {
			t.Fatalf("%s\n%v", where, err)
		}
		synctest.Wait() // every response to the step has been sent
		if err := r.check(s); err != nil {
			t.Fatalf("%s\n%v", where, err)
		}
	}
}

// newRun makes the Source and the Server for a vector, and its Envoys.
func newRun(ctx context.Context, v *vector, protocol string) (*run, error) {
	if protocol != "sotw" && protocol != "delta" {
		return nil, fmt.Errorf("unknown protocol %q", protocol)
	}
	r := &run{delta: protocol == "delta", ctx: ctx, labels: map[string]*received{}}
	views := map[string]string{}
	configs := map[string]xds.EnvoyConfig[string]{}
	for _, e := range v.Envoys {
		views[e.Name] = e.View
		cfg := xds.EnvoyConfig[string]{Details: e.Name}
		if e.SubscribesTo != nil {
			cfg.SubscribesTo = []xds.TypeURL{} // empty means no type, where nil means every type served
		}
		for _, name := range e.SubscribesTo {
			typ, err := typeURL(name)
			if err != nil {
				return nil, err
			}
			cfg.SubscribesTo = append(cfg.SubscribesTo, typ)
		}
		if e.DelayUpdatesUntilAckOf != "" {
			typ, err := typeURL(e.DelayUpdatesUntilAckOf)
			if err != nil {
				return nil, err
			}
			cfg.DelayUpdatesUntilAckOf = typ
		}
		configs[e.Name] = cfg
		r.envoys = append(r.envoys, &envoy{name: e.Name})
	}
	var served []xds.TypeURL
	for _, name := range v.Serves {
		typ, err := typeURL(name)
		if err != nil {
			return nil, err
		}
		served = append(served, typ)
	}
	r.src = snapshot.New[string, string](func(details string) string { return views[details] })
	srv, err := xds.NewServer(xds.ServerConfig[*snapshot.Update, string]{
		Source:   r.src,
		Builders: snapshot.Builders[string](served...),
		ConfigFor: func(_ context.Context, node *corev3.Node) (xds.EnvoyConfig[string], error) {
			cfg, ok := configs[node.GetId()]
			if !ok {
				return cfg, fmt.Errorf("no envoy %q", node.GetId())
			}
			return cfg, nil
		},
		Logger: slog.New(slog.DiscardHandler),
	})
	if err != nil {
		return nil, err
	}
	r.srv = srv
	return r, nil
}

func (r *run) perform(s step) error {
	switch {
	case s.Update != nil:
		return r.update(s.Update)
	case s.Request != "":
		return r.request(s)
	case s.Disconnect != "":
		e, err := r.envoy(s.Disconnect)
		if err != nil {
			return err
		}
		if e.conn == nil {
			return fmt.Errorf("%s has no stream to close", e.name)
		}
		e.conn.closeSend()
		e.conn.closing = true
		return nil
	}
	return errors.New("unknown step")
}

type change = snapshot.Change[string, string]

// update makes one Source update. Resources are parsed first, as the Source is locked while the update runs.
func (r *run) update(ops []json.RawMessage) error {
	var edits []func(*change)
	for _, raw := range ops {
		var then string
		if json.Unmarshal(raw, &then) == nil {
			if then != "then" {
				return fmt.Errorf("unknown update op %s", raw)
			}
			edits = append(edits, (*change).Then)
			continue
		}
		var o op
		if err := strict(raw, &o); err != nil {
			return fmt.Errorf("update op %s: %w", raw, err)
		}
		switch {
		case o.SetView != "":
			edits = append(edits, func(c *change) { c.SetView(o.SetView, o.Groups...) })
		case o.EditGroup != "":
			puts, err := parseAll(o.Put)
			if err != nil {
				return err
			}
			removes := map[xds.TypeURL][]string{}
			for name, ns := range o.Remove {
				typ, err := typeURL(name)
				if err != nil {
					return err
				}
				removes[typ] = ns
			}
			edits = append(edits, func(c *change) {
				c.EditGroup(o.EditGroup, func(e *snapshot.Editor) {
					for _, m := range puts {
						e.Put(m)
					}
					for typ, ns := range removes {
						for _, n := range ns {
							e.Remove(typ, n)
						}
					}
				})
			})
		case o.SetGroup != "":
			resources, err := parseAll(o.Resources)
			if err != nil {
				return err
			}
			e := snapshot.Empty().Edit()
			for _, m := range resources {
				e.Put(m)
			}
			group := e.Build()
			edits = append(edits, func(c *change) { c.SetGroup(o.SetGroup, group) })
		case o.RemoveGroup != "":
			edits = append(edits, func(c *change) { c.RemoveGroup(o.RemoveGroup) })
		default:
			return fmt.Errorf("unknown update op %s", raw)
		}
	}
	r.src.Update(func(c *change) {
		for _, edit := range edits {
			edit(c)
		}
	})
	return nil
}

func (r *run) request(s step) error {
	e, err := r.envoy(s.Envoy)
	if err != nil {
		return err
	}
	typ, err := typeURL(s.Request)
	if err != nil {
		return err
	}
	if s.Ack != "" && s.Nack != "" {
		return errors.New("a request can't both ACK and NACK")
	}
	if e.conn == nil {
		if r.delta {
			e.conn = dial(r.ctx, r.srv.DeltaAggregatedResources, deltaReceived)
		} else {
			e.conn = dial(r.ctx, r.srv.StreamAggregatedResources, sotwReceived)
		}
	}
	c := e.conn
	var answering *received
	if ref := cmp.Or(s.Ack, s.Nack); ref != "" {
		answering = c.last[typ]
		if ref != "last" {
			answering = r.labels[ref]
		}
		if answering == nil && r.replaying && ref == "last" {
			s.Ack, s.Nack = "", "" // replaying, an answer to no response is left out
		} else if answering == nil {
			return fmt.Errorf("no response %s of %s to answer", ref, s.Request)
		} else if answering.typ != typ {
			return fmt.Errorf("%s is a %s response, not %s", ref, typeName(answering.typ), s.Request)
		}
	}
	var detail *statuspb.Status
	if s.Nack != "" {
		detail = &statuspb.Status{Message: "rejected, as the vector says"}
	}
	var node *corev3.Node
	if c.first {
		node, c.first = &corev3.Node{Id: e.name}, false
	}

	if r.delta {
		req := &discoveryv3.DeltaDiscoveryRequest{Node: node, TypeUrl: string(typ), ResourceNamesSubscribe: s.Subscribe,
			ResourceNamesUnsubscribe: s.Unsubscribe, ErrorDetail: detail}
		for _, n := range s.Kept {
			if req.InitialResourceVersions == nil {
				req.InitialResourceVersions = map[string]string{}
			}
			req.InitialResourceVersions[n] = "kept" // ports compare names only
		}
		if answering != nil {
			req.ResponseNonce = answering.nonce
		}
		return c.send(req)
	}

	if s.Kept != nil {
		return errors.New("kept is for delta only")
	}
	names := c.names[typ]
	for _, n := range s.Subscribe {
		if !slices.Contains(names, n) {
			names = append(names, n)
		}
	}
	names = slices.DeleteFunc(names, func(n string) bool { return slices.Contains(s.Unsubscribe, n) })
	c.names[typ] = names
	req := &discoveryv3.DiscoveryRequest{Node: node, TypeUrl: string(typ), ResourceNames: slices.Clone(names), ErrorDetail: detail}
	// Each request answers the last response of its type, unless the step says which, as Envoy does.
	if answers := cmp.Or(answering, c.last[typ]); answers != nil {
		req.ResponseNonce = answers.nonce
	}
	if s.Ack != "" {
		c.acked[typ] = answering.version
	}
	req.VersionInfo = c.acked[typ]
	return c.send(req)
}

// check checks each Envoy got exactly the responses the step expects of it, in order.
func (r *run) check(s step) error {
	want := map[*envoy][]response{}
	for _, resp := range s.Expect {
		e, err := r.envoy(cmp.Or(resp.Envoy, s.Envoy)) // a request's responses go to its Envoy
		if err != nil {
			return err
		}
		want[e] = append(want[e], resp)
	}
	var report strings.Builder
	matched := true
	for _, e := range r.envoys {
		var responses []*received
		if c := e.conn; c != nil {
			var err error
			if responses, err = c.take(); err != nil {
				return fmt.Errorf("envoy %s: %w", e.name, err)
			}
			select {
			case err := <-c.ended:
				if !c.closing || err != nil {
					return fmt.Errorf("the server ended envoy %s's stream: %v", e.name, err)
				}
				e.conn = nil
			default:
				if c.closing {
					return fmt.Errorf("envoy %s's stream didn't end when it closed it", e.name)
				}
			}
		}
		var expected []got
		for _, resp := range want[e] {
			g, err := r.expected(resp)
			if err != nil {
				return err
			}
			expected = append(expected, g)
		}
		actual := make([]got, len(responses))
		for i, rcv := range responses {
			actual[i] = rcv.got
		}
		ok := slices.EqualFunc(expected, actual, got.equal)
		matched = matched && ok
		fmt.Fprintf(&report, "envoy %s expected:\n%s\nbut got:\n%s\n", e.name, show(expected), show(actual))
		if ok {
			for i, resp := range want[e] {
				if resp.As == "" {
					continue
				}
				if _, dup := r.labels[resp.As]; dup {
					return fmt.Errorf("label %s is used twice", resp.As)
				}
				r.labels[resp.As] = responses[i]
			}
		}
	}
	if !matched {
		return errors.New(report.String())
	}
	return nil
}

// expected is what a response should hold on this run's protocol: the base fields, with the protocol's own replacing
// them.
func (r *run) expected(resp response) (got, error) {
	c, override := resp.contents, resp.Sotw
	if r.delta {
		override = resp.Delta
	}
	if override != nil {
		if override.Resources != nil {
			c.Resources = override.Resources
		}
		if override.Removed != nil {
			c.Removed = override.Removed
		}
	}
	if !r.delta && len(c.Removed) > 0 {
		return got{}, fmt.Errorf("a SotW response can't have removed: %v", c.Removed)
	}
	typ, err := typeURL(resp.Type)
	if err != nil {
		return got{}, err
	}
	ms, err := parse(resp.Type, c.Resources)
	if err != nil {
		return got{}, err
	}
	g := newGot(typ, c.Removed)
	for _, m := range ms {
		g.add(nameOf(m), m)
	}
	return g, nil
}

func (r *run) envoy(name string) (*envoy, error) {
	if name == "" {
		if len(r.envoys) != 1 {
			return nil, errors.New("there are several envoys, so say which")
		}
		return r.envoys[0], nil
	}
	for _, e := range r.envoys {
		if e.name == name {
			return e, nil
		}
	}
	return nil, fmt.Errorf("no envoy %s", name)
}

// got is what a response holds that a vector compares: its type, its resources by name, and the names it removes.
type got struct {
	typ       xds.TypeURL
	resources map[string]proto.Message
	removed   []string // sorted
}

func newGot(typ xds.TypeURL, removed []string) got {
	return got{typ: typ, resources: map[string]proto.Message{}, removed: slices.Sorted(slices.Values(removed))}
}

func (g got) add(name string, m proto.Message) {
	if _, ok := g.resources[name]; ok {
		name += " (again)" // so it shows as a mismatch
	}
	g.resources[name] = m
}

func (g got) equal(o got) bool {
	return g.typ == o.typ && slices.Equal(g.removed, o.removed) && maps.EqualFunc(g.resources, o.resources, proto.Equal)
}

func (g got) String() string {
	shown := struct {
		Type      string            `json:"type"`
		Resources []json.RawMessage `json:"resources,omitempty"`
		Removed   []string          `json:"removed,omitempty"`
	}{Type: typeName(g.typ), Removed: g.removed}
	for _, name := range slices.Sorted(maps.Keys(g.resources)) {
		shown.Resources = append(shown.Resources, json.RawMessage(protojson.Format(g.resources[name])))
	}
	js, err := json.MarshalIndent(shown, "  ", "  ")
	if err != nil {
		return err.Error()
	}
	return "  " + string(js)
}

func show(gs []got) string {
	if len(gs) == 0 {
		return "  nothing"
	}
	var ss []string
	for _, g := range gs {
		ss = append(ss, g.String())
	}
	return strings.Join(ss, "\n")
}
