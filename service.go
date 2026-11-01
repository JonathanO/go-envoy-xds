package xds

import (
	"log/slog"
	"strconv"
)

// A service serves one type on one stream, SotW or delta. The manager says when to send what.
type service[R request, U any] interface {
	typeURL() TypeURL
	// process handles a request for its type: an ACK or NACK, a subscription change, or both.
	process(req R)
	awaitingAck() bool
	// addsToCome reports whether the staged change's add round is still to be sent.
	addsToCome() bool
	init(state U)
	// stage takes the next change, to send in sendAdds and sendRemoves.
	stage(change U)
	sendAdds()
	sendRemoves()
	// abandon drops what's left of the staged change, so its remove round never goes out. Called when Envoy NACKs.
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
	typ     TypeURL
	builder ConfigBuilder[U, D] // nil if nothing serves this type: it never has resources
	details D
	log     *slog.Logger
	state   U
	round   round // of the staged change
	active  bool  // Envoy has asked for this type
	version int   // of the last response: 0 until we've sent Envoy something of this type
	// Stale names a reconnecting delta Envoy kept, to remove in remove order.
	deferred set
}

func (b *base[U, D]) typeURL() TypeURL  { return b.typ }
func (b *base[U, D]) init(state U)      { b.state = state }
func (b *base[U, D]) served() bool      { return b.builder != nil }
func (b *base[U, D]) hasAnswered() bool { return b.version > 0 }
func (b *base[U, D]) addsToCome() bool  { return b.round == adding }

func (b *base[U, D]) deferredRemovals() bool { return len(b.deferred) > 0 }

// abandon stops the staged change where it is. A type still mid-change stays so for good: its remove round is never
// sent, so what it would remove stays in answers, as Envoy may still use it. After a NACK only answers are sent.
func (b *base[U, D]) abandon() {
	if b.round != idle {
		b.round = removing
	}
}

func (b *base[U, D]) stage(change U) {
	b.state = change
	b.round = adding
}

// resources is every subscribed resource as things stand. Mid-change, that's the add round's: what the change removes
// stays until its type's remove round is sent, so nothing goes before what may refer to it.
func (b *base[U, D]) resources(subscribed func(string) bool) []Resource {
	if b.builder == nil {
		return nil
	}
	var rs []Resource
	if b.round != idle {
		rs = b.builder.ResourcesAddOrder(b.state, subscribed, b.details)
	} else {
		rs = b.builder.ResourcesRemoveOrder(b.state, subscribed, b.details)
	}
	b.check(rs, nil, subscribed)
	return rs
}

// adds is the staged change's add round, if it's not been sent. Either way, the remove round is next.
func (b *base[U, D]) adds(subscribed func(string) bool) (r Response) {
	if b.round != adding {
		return
	}
	b.round = removing
	// Not asked for this type: don't ask the builder, which might do work for nothing, or return resources Envoy
	// would reject.
	if !b.active || b.builder == nil {
		return
	}
	r = b.builder.AddOrder(b.state, subscribed, b.details)
	b.check(r.Updates, r.Removes, subscribed)
	return r
}

// removes is the staged change's remove round. It's sent once, after the add round.
func (b *base[U, D]) removes(subscribed func(string) bool) (r Response) {
	b.round = idle
	if !b.active || b.builder == nil {
		return
	}
	r = b.builder.RemoveOrder(b.state, subscribed, b.details)
	b.check(r.Updates, r.Removes, subscribed)
	return r
}

// nextVersion is the next response's version, which is also its nonce. Each type's count up from 1.
func (b *base[U, D]) nextVersion() string {
	b.version++
	return strconv.Itoa(b.version)
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
