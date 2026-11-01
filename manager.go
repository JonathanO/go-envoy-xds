package xds

import (
	"log/slog"
	"slices"
	"sync"
	"time"

	corev3 "github.com/envoyproxy/go-control-plane/envoy/config/core/v3"
	statuspb "google.golang.org/genproto/googleapis/rpc/status"
)

// request is what SotW and delta requests have in common.
type request interface {
	GetTypeUrl() string
	GetNode() *corev3.Node
	GetErrorDetail() *statuspb.Status
}

// usesClusters are the types that refer to clusters. Envoy re-subscribes to everything at once when it reconnects, and
// may subscribe at any point in a change, so their answers wait until the clusters and endpoints they may use have
// been sent and ACKed. CDS and EDS answers never wait: Envoy holds its CDS ACK until new clusters have warmed, which
// takes the EDS answer.
func usesClusters(t TypeURL) bool {
	switch t {
	case ListenerType, RouteType, ScopedRouteType, VirtualHostType:
		return true
	}
	return false
}

// A manager sequences one stream's services: it pushes each change make before break, one type at a time, waiting for
// Envoy to ACK each before the next. Only the stream's goroutine calls it.
type manager[R request, U any] struct {
	services    map[TypeURL]service[R, U]
	adds        []service[R, U] // in add order
	removes     []service[R, U] // in remove order
	mailbox     *mailbox[U]
	unsubscribe func() // from the Source
	metrics     Metrics
	log         *slog.Logger

	pushing   bool  // a change is under way
	round     round // of the change under way
	next      int   // index in the round's order of the next service to send
	waitingOn service[R, U]
	// Services, other than waitingOn, waiting for Envoy to ACK an answer to a subscription.
	outstanding map[service[R, U]]struct{}

	// Envoy rejected something, so this stream gets no more changes. See freeze.
	frozen          bool
	delayUntilAckOf TypeURL
	subscribesTo    []TypeURL // nil for all
	held            []R       // requests waiting for clusters to be ACKed: see usesClusters
	started         time.Time
	// Stale removals waited too long for an ACK once, so they always have: see mayRemove.
	gaveUpWaitingToRemove bool
}

func newManager[R request, U, D any](services map[TypeURL]service[R, U], cfg EnvoyConfig[D], mb *mailbox[U], metrics Metrics, log *slog.Logger) *manager[R, U] {
	m := &manager[R, U]{
		services:        services,
		mailbox:         mb,
		metrics:         metrics,
		log:             log,
		outstanding:     map[service[R, U]]struct{}{},
		delayUntilAckOf: cfg.DelayUpdatesUntilAckOf,
		subscribesTo:    cfg.SubscribesTo,
	}
	for _, t := range addOrder {
		m.adds = append(m.adds, services[t])
	}
	for _, t := range removeOrder {
		m.removes = append(m.removes, services[t])
	}
	if m.delayUntilAckOf != "" && m.subscribesTo != nil && !slices.Contains(m.subscribesTo, m.delayUntilAckOf) {
		log.Warn("DelayUpdatesUntilAckOf names a type this Envoy doesn't subscribe to, so not delaying updates", "type", m.delayUntilAckOf)
		m.delayUntilAckOf = ""
	}
	return m
}

// init sets the state all later changes follow on from, and how to stop them.
func (m *manager[R, U]) init(state U, unsubscribe func()) {
	m.started = time.Now()
	m.unsubscribe = sync.OnceFunc(unsubscribe)
	for t, s := range m.services {
		s.init(state)
		s.setDeferRemovals(func() bool { return !m.mayRemove(t) })
	}
}

// process handles a request for a type we know.
func (m *manager[R, U]) process(req R) {
	if m.mustWait(req) {
		if req.GetErrorDetail() != nil {
			m.freeze() // at once, though what it asks for waits its turn
		}
		m.held = append(m.held, req)
		return
	}
	m.handle(req)
	for len(m.held) > 0 && !m.clustersPending() {
		req := m.held[0]
		m.held = m.held[1:]
		m.handle(req)
	}
}

// mustWait reports whether to answer a request later, in add order: see usesClusters. It never overtakes a held request
// of its type, as each follows on from the last. Otherwise an ACK its service is waiting for never waits.
func (m *manager[R, U]) mustWait(req R) bool {
	t := TypeURL(req.GetTypeUrl())
	if slices.ContainsFunc(m.held, func(h R) bool { return TypeURL(h.GetTypeUrl()) == t }) {
		return true
	}
	return usesClusters(t) && !m.services[t].awaitingAck() && m.clustersPending()
}

func (m *manager[R, U]) handle(req R) {
	s := m.services[TypeURL(req.GetTypeUrl())]
	s.process(req)
	if req.GetErrorDetail() != nil {
		m.freeze()
	}
	m.updateOutstanding(s)
	if m.pushing {
		m.continuePush() // it may be the ACK the push waits for
	} else {
		m.pushAllowedWork()
	}
}

// updatesWaiting is called when the mailbox may have updates.
func (m *manager[R, U]) updatesWaiting() {
	if !m.pushing {
		m.pushAllowedWork() // stale removals go first, then the change
	}
}

// close is called when the stream ends.
func (m *manager[R, U]) close() {
	m.unsubscribe()
	m.metrics.Closed()
}

// clustersPending reports whether clusters or endpoints are still to be sent or ACKed: a change's, or an answer's. The
// endpoints' add round comes after the clusters', so while either is to come, the endpoints' is.
func (m *manager[R, U]) clustersPending() bool {
	c, e := m.services[ClusterType], m.services[EndpointType]
	return c.awaitingAck() || e.awaitingAck() || e.addsToCome()
}

// mayRemove reports whether stale resources of this type, kept by a reconnecting Envoy, may be removed now: every type
// before it in remove order that's served and this Envoy gets has been answered and ACKed, or we've waited long enough.
func (m *manager[R, U]) mayRemove(t TypeURL) bool {
	if m.gaveUpWaitingToRemove {
		return true
	}
	for _, earlier := range m.removes {
		if earlier.typeURL() == t {
			break
		}
		used := earlier.served() && (m.subscribesTo == nil || slices.Contains(m.subscribesTo, earlier.typeURL()))
		if used && (!earlier.hasAnswered() || earlier.awaitingAck()) {
			if !m.waitedTooLong() {
				return false
			}
			m.gaveUpWaitingToRemove = true
			m.log.Warn("No answer to or ACK of a type, so removing stale resources anyway: does this Envoy subscribe to it? See EnvoyConfig.SubscribesTo",
				"waited_for", earlier.typeURL(), "after", waitForAck, "removing", t)
			break
		}
	}
	return true
}

func (m *manager[R, U]) updateOutstanding(s service[R, U]) {
	_, was := m.outstanding[s]
	switch {
	case s.awaitingAck() && !was:
		m.outstanding[s] = struct{}{}
		m.metrics.AwaitingAck()
	case !s.awaitingAck() && was:
		delete(m.outstanding, s)
		m.metrics.Acked()
		if s.typeURL() == m.delayUntilAckOf {
			m.delayUntilAckOf = ""
		}
	}
}

// freeze stops sending changes on this stream, for good. Envoy keeps serving its last good config until it reconnects.
//
// A rejected change won't fix itself: resending it, or any later change that includes it, gets rejected again. Envoy
// may also have applied part of it, so we no longer know what it has, and diffing from what we think it has would make
// things worse. Dropping the stream instead would have Envoy reconnect straight away, be sent the same config, and
// loop. Subscription requests are still answered, from our own state: the rejected change's add round, as its removals
// were never sent and Envoy may still use what they'd remove. For SotW that includes what Envoy rejected, as each
// answer is the whole set: resending it changes nothing Envoy has, where rolling it back would undo any part of it
// Envoy took.
func (m *manager[R, U]) freeze() {
	if m.frozen {
		return
	}
	m.frozen = true
	m.unsubscribe()
	if m.pushing {
		m.pushing = false
		m.waitingOn = nil
		for _, s := range m.services {
			s.abandon()
		}
		m.metrics.PushCompleted()
	}
	m.metrics.Frozen()
}

func (m *manager[R, U]) waitedTooLong() bool { return time.Since(m.started) >= waitForAck }

func (m *manager[R, U]) pushAllowed() bool {
	if m.delayUntilAckOf != "" && m.waitedTooLong() {
		m.log.Warn("No ACK of the type updates wait for, so sending them anyway: does this Envoy subscribe to it? See EnvoyConfig",
			"type", m.delayUntilAckOf, "after", waitForAck)
		m.delayUntilAckOf = ""
	}
	return len(m.outstanding) == 0 && m.delayUntilAckOf == ""
}

func (m *manager[R, U]) pushAllowedWork() {
	// Nor while requests are held: a type that hasn't heard its first one would be skipped, though Envoy has some of it.
	if m.frozen || len(m.outstanding) > 0 || len(m.held) > 0 {
		return
	}
	m.pushDeferredRemovals() // the ACK one waits for holds the change too: see pushAllowed
	if !m.pushAllowed() {
		return
	}
	change, ok := m.mailbox.take()
	if !ok {
		return
	}
	m.metrics.PushBegan()
	m.pushing = true
	m.round = adding
	m.next = 0
	for _, s := range m.services {
		s.stage(change)
	}
	m.continuePush()
}

func (m *manager[R, U]) pushDeferredRemovals() {
	for _, s := range m.removes {
		if !s.deferredRemovals() {
			continue
		}
		if !m.mayRemove(s.typeURL()) {
			return // and not a later type's: removals go in remove order
		}
		s.sendDeferredRemovals()
		if s.awaitingAck() {
			m.outstanding[s] = struct{}{} // it was empty: see pushAllowedWork
			m.metrics.AwaitingAck()
			return
		}
	}
}

// continuePush sends the change under way, one service at a time, as far as it can without waiting for an ACK.
func (m *manager[R, U]) continuePush() {
	for {
		if m.waitingOn != nil && m.waitingOn.awaitingAck() {
			return
		}
		s := m.nextService()
		if s == nil && m.round == adding {
			m.round = removing
			m.next = 0
			s = m.nextService()
		}
		m.waitingOn = s
		if s == nil {
			m.metrics.PushCompleted()
			m.pushing = false
			m.pushAllowedWork()
			return
		}
		if m.round == adding {
			s.sendAdds()
		} else {
			s.sendRemoves()
		}
	}
}

func (m *manager[R, U]) nextService() service[R, U] {
	order := m.adds
	if m.round == removing {
		order = m.removes
	}
	if m.next >= len(order) {
		return nil
	}
	m.next++
	return order[m.next-1]
}
