package snapshot

import (
	"fmt"
	"iter"
	"maps"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"

	xds "github.com/JonathanO/go-envoy-xds"
)

type names = map[string]struct{}

// A Snapshot is immutable desired config: resources by type, then by name.
//
// Make new snapshots from old ones with Edit. What you don't change is shared with the old one. Change.EditGroup also
// records the names an edit touches, so working out what changed costs as much as the change, not the whole config;
// Change.SetGroup compares every name.
type Snapshot struct {
	resources map[xds.TypeURL]map[string]proto.Message
}

var empty = &Snapshot{}

// Empty returns the snapshot with nothing in it, to edit from.
func Empty() *Snapshot { return empty }

// Resource returns the resource of that type and name, if there is one.
func (s *Snapshot) Resource(t xds.TypeURL, name string) (proto.Message, bool) {
	m, ok := s.resources[t][name]
	return m, ok
}

// Resources yields every resource of that type, by name.
func (s *Snapshot) Resources(t xds.TypeURL) iter.Seq2[string, proto.Message] {
	return maps.All(s.resources[t])
}

// Types yields the types it has resources of.
func (s *Snapshot) Types() iter.Seq[xds.TypeURL] {
	return maps.Keys(s.resources)
}

// Edit returns an editor of a new snapshot, starting as this one.
func (s *Snapshot) Edit() *Editor {
	return &Editor{from: s, edited: map[xds.TypeURL]map[string]proto.Message{}, touched: map[xds.TypeURL]names{}}
}

// An Editor makes a Snapshot. It's for one goroutine at a time.
type Editor struct {
	from    *Snapshot
	edited  map[xds.TypeURL]map[string]proto.Message
	touched map[xds.TypeURL]names // names changed since Edit
}

// Put adds or replaces a resource, under the name Envoy knows it by: its name field, or cluster_name for a
// ClusterLoadAssignment. It panics for a resource with neither: use PutNamed.
func (e *Editor) Put(m proto.Message) {
	e.PutNamed(nameOf(m), m)
}

// PutNamed adds or replaces a resource, under a name you give.
func (e *Editor) PutNamed(name string, m proto.Message) {
	t := xds.TypeOf(m)
	if proto.Equal(e.current(t)[name], m) {
		return
	}
	e.mutable(t)[name] = m
	e.touch(t, name)
}

// Remove removes the resource of that type and name, if there is one.
func (e *Editor) Remove(t xds.TypeURL, name string) {
	if _, ok := e.current(t)[name]; !ok {
		return
	}
	delete(e.mutable(t), name)
	e.touch(t, name)
}

// Build returns the snapshot. Edits after it carry on from it, and don't change it.
func (e *Editor) Build() *Snapshot {
	s := &Snapshot{resources: maps.Clone(e.from.resources)}
	if s.resources == nil {
		s.resources = map[xds.TypeURL]map[string]proto.Message{}
	}
	for t, rs := range e.edited {
		if len(rs) == 0 {
			delete(s.resources, t)
		} else {
			s.resources[t] = rs
		}
	}
	e.from, e.edited = s, map[xds.TypeURL]map[string]proto.Message{} // the snapshot owns those now
	return s
}

func (e *Editor) touch(t xds.TypeURL, name string) {
	if e.touched[t] == nil {
		e.touched[t] = names{}
	}
	e.touched[t][name] = struct{}{}
}

func (e *Editor) current(t xds.TypeURL) map[string]proto.Message {
	if rs, ok := e.edited[t]; ok {
		return rs
	}
	return e.from.resources[t]
}

// ponytail: copies a type's map on its first edit, so O(resources of that type) per edited type; use a persistent map
// if that shows in profiles.
func (e *Editor) mutable(t xds.TypeURL) map[string]proto.Message {
	rs, ok := e.edited[t]
	if !ok {
		rs = maps.Clone(e.from.resources[t])
		if rs == nil {
			rs = map[string]proto.Message{}
		}
		e.edited[t] = rs
	}
	return rs
}

// nameOf is the name Envoy knows a resource by.
func nameOf(m proto.Message) string {
	fields := m.ProtoReflect().Descriptor().Fields()
	for _, name := range []protoreflect.Name{"name", "cluster_name"} {
		if f := fields.ByName(name); f != nil && f.Kind() == protoreflect.StringKind && f.Cardinality() != protoreflect.Repeated {
			return m.ProtoReflect().Get(f).String()
		}
	}
	panic(fmt.Sprintf("snapshot: can't tell the name of a %s: use PutNamed", m.ProtoReflect().Descriptor().FullName()))
}
