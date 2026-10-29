package xds

import (
	"log/slog"

	"google.golang.org/protobuf/proto"
)

// A service serves one type on one stream, SotW or delta. The manager says when to send what.
type service[R request, U any] interface {
	typeURL() TypeURL
	// process handles a request for its type: an ACK or NACK, a subscription change, or both.
	process(req R) error
	awaitingAck() bool
	init(state U)
	// stage takes the next change, to send in sendAdds and sendRemoves.
	stage(change U)
	sendAdds()
	sendRemoves()
	// abandon drops what's left of the staged change. Called when Envoy NACKs.
	abandon()
	served() bool
	hasAnswered() bool
	// Removals of what a reconnecting delta Envoy kept, and we no longer have, wait for the manager to send them in
	// remove order. setDeferRemovals says whether they must wait when Envoy subscribes.
	setDeferRemovals(func() bool)
	deferredRemovals() bool
	sendDeferredRemovals()
}

type round int

const (
	idle round = iota
	adding
	removing
)

// base is what SotW and delta services share.
type base[U, D any] struct {
	typ      TypeURL
	builder  ConfigBuilder[U, D] // nil if nothing serves this type: it never has resources
	details  D
	log      *slog.Logger
	state    U
	round    round // of the staged change
	active   bool  // Envoy has asked for this type
	answered bool  // we've sent Envoy something of it
}

func (b *base[U, D]) typeURL() TypeURL  { return b.typ }
func (b *base[U, D]) init(state U)      { b.state = state }
func (b *base[U, D]) served() bool      { return b.builder != nil }
func (b *base[U, D]) hasAnswered() bool { return b.answered }
func (b *base[U, D]) abandon()          { b.round = idle }

func (b *base[U, D]) stage(change U) {
	b.state = change
	b.round = adding
}

// resources is every subscribed resource as things stand: mid-change during the add round.
func (b *base[U, D]) resources(subscribed func(string) bool) []Resource {
	if b.builder == nil {
		return nil
	}
	var rs []Resource
	if b.round == adding {
		rs = b.builder.ResourcesAddOrder(b.state, subscribed, b.details)
	} else {
		rs = b.builder.ResourcesRemoveOrder(b.state, subscribed, b.details)
	}
	b.check(rs, nil, subscribed)
	return rs
}

// adds is the staged change's add round, ok if there's one to send. Either way, the remove round is next.
func (b *base[U, D]) adds(subscribed func(string) bool) (r Response, ok bool) {
	if b.round != adding {
		return r, false
	}
	b.round = removing
	// Not asked for this type: don't ask the builder, which might do work for nothing, or return resources Envoy
	// would reject.
	if !b.active || b.builder == nil {
		return r, false
	}
	r = b.builder.AddOrder(b.state, subscribed, b.details)
	b.check(r.Updates, r.Removes, subscribed)
	return r, true
}

// removes is the staged change's remove round, ok if there's one to send.
func (b *base[U, D]) removes(subscribed func(string) bool) (r Response, ok bool) {
	if b.round != removing {
		return r, false
	}
	b.round = idle
	if !b.active || b.builder == nil {
		return r, false
	}
	r = b.builder.RemoveOrder(b.state, subscribed, b.details)
	b.check(r.Updates, r.Removes, subscribed)
	return r, true
}

func (b *base[U, D]) check(rs []Resource, removes []string, subscribed func(string) bool) {
	for _, r := range rs {
		if !subscribed(r.Name) {
			b.log.Warn("Config builder returned a resource Envoy didn't ask for", "type", b.typ, "name", r.Name)
			return
		}
	}
	for _, n := range removes {
		if !subscribed(n) {
			b.log.Warn("Config builder removed a resource Envoy didn't ask for", "type", b.typ, "name", n)
			return
		}
	}
}

// same reports whether two resources are equal, cheaply when they're one message.
func same(a, b proto.Message) bool {
	return a == b || (b != nil && proto.Equal(a, b))
}
