package snapshot

import (
	xds "github.com/JonathanO/go-envoy-xds"
)

// Builders returns a ConfigBuilder for each type, serving it from each Envoy's view of a Source.
//
// Added and changed resources go out in the add round, removed ones in the remove round. As the Server's add order
// puts what's referred to before what refers to it, one Source update is make before break with no ordering of your
// own.
func Builders[D any](types ...xds.TypeURL) []xds.ConfigBuilder[*Update, D] {
	bs := make([]xds.ConfigBuilder[*Update, D], len(types))
	for i, t := range types {
		bs[i] = builder[D]{t}
	}
	return bs
}

type builder[D any] struct {
	typ xds.TypeURL
}

func (b builder[D]) Type() xds.TypeURL { return b.typ }

func (b builder[D]) AddOrder(u *Update, subscribed func(string) bool, _ D) xds.Response {
	var r xds.Response
	for name := range u.candidates(b.typ) {
		if !subscribed(name) {
			continue
		}
		// Shared resources are one message, so most compare cheaply.
		if after := u.after(b.typ, name); after != nil && !sameMessage(after, u.before(b.typ, name)) {
			r.Updates = append(r.Updates, xds.Resource{Name: name, Message: after})
		}
	}
	return r
}

func (b builder[D]) RemoveOrder(u *Update, subscribed func(string) bool, _ D) xds.Response {
	var r xds.Response
	for name := range u.candidates(b.typ) {
		if subscribed(name) && u.after(b.typ, name) == nil && u.before(b.typ, name) != nil {
			r.Removes = append(r.Removes, name)
		}
	}
	return r
}

// ResourcesAddOrder is the state mid-change: everything new is in, nothing old has gone yet.
func (b builder[D]) ResourcesAddOrder(u *Update, subscribed func(string) bool, _ D) []xds.Resource {
	var rs []xds.Resource
	after := u.current.all(b.typ)
	for name, m := range after {
		if subscribed(name) {
			rs = append(rs, xds.Resource{Name: name, Message: m})
		}
	}
	for name, m := range u.previous.all(b.typ) {
		if _, ok := after[name]; !ok && subscribed(name) {
			rs = append(rs, xds.Resource{Name: name, Message: m})
		}
	}
	return rs
}

func (b builder[D]) ResourcesRemoveOrder(u *Update, subscribed func(string) bool, _ D) []xds.Resource {
	var rs []xds.Resource
	for name, m := range u.current.all(b.typ) {
		if subscribed(name) {
			rs = append(rs, xds.Resource{Name: name, Message: m})
		}
	}
	return rs
}
