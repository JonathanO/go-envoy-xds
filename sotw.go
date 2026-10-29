package xds

import (
	"cmp"
	"maps"
	"slices"
	"strconv"

	discoveryv3 "github.com/envoyproxy/go-control-plane/envoy/service/discovery/v3"
	"google.golang.org/protobuf/types/known/anypb"
)

// sotwService serves one type over SotW, where each response is every resource Envoy is subscribed to.
type sotwService[U, D any] struct {
	base[U, D]
	send func(*discoveryv3.DiscoveryResponse)
	fail func(error)
	subs sotwSubs
	have map[string]Resource // what Envoy has, or will have once it ACKs

	version  uint64 // of the last response; also its nonce
	accepted uint64 // the last version Envoy ACKed
	awaiting uint64 // the version we're waiting for Envoy to answer, or 0
}

func (s *sotwService[U, D]) process(req *discoveryv3.DiscoveryRequest) error {
	s.active = true
	if s.version != 0 {
		if req.GetResponseNonce() != strconv.FormatUint(s.version, 10) {
			s.log.Debug("Ignoring a request answering an earlier response", "type", s.typ, "nonce", req.GetResponseNonce())
			return nil
		}
		switch {
		case req.GetErrorDetail() != nil:
			s.awaiting = 0 // answered, if not how we'd like: the manager sends no more changes
		case req.GetVersionInfo() == strconv.FormatUint(s.version, 10):
			s.accepted = s.version
		}
	}
	if added, changed := s.subs.update(req.GetResourceNames()); changed {
		s.subscriptionChanged(added)
	}
	return nil
}

func (s *sotwService[U, D]) subscriptionChanged(added func(string) bool) {
	maps.DeleteFunc(s.have, func(name string, _ Resource) bool { return !s.subs.subscribed(name) })
	if added == nil {
		// Envoy drops what it unsubscribed from itself and waits for no answer, so don't make it ACK the rest again,
		// e.g. on every EDS unsubscribe after a cluster removal. Names can only be dropped once they were added, and
		// every request that added names was answered.
		return
	}
	for _, r := range s.resources(added) {
		s.have[r.Name] = r
	}
	// Mid-change, the answer has the change so far, so its round needn't be sent again.
	switch s.round {
	case adding:
		if r, ok := s.adds(s.subs.subscribed); ok {
			s.apply(r)
		}
	case removing:
		if r, ok := s.removes(s.subs.subscribed); ok {
			s.apply(r)
		}
	}
	// Answer even if nothing changed: Envoy waits for an answer to its first request of each type, e.g. CDS when
	// there are no clusters yet, and to hear that names it asked for don't exist.
	s.push()
}

func (s *sotwService[U, D]) apply(r Response) bool {
	if r.empty() {
		return false
	}
	for _, u := range r.Updates {
		s.have[u.Name] = u
	}
	for _, n := range r.Removes {
		delete(s.have, n)
	}
	return true
}

func (s *sotwService[U, D]) sendAdds() {
	if r, ok := s.adds(s.subs.subscribed); ok && s.apply(r) {
		s.push()
	}
}

func (s *sotwService[U, D]) sendRemoves() {
	if r, ok := s.removes(s.subs.subscribed); ok && s.apply(r) {
		s.push()
	}
}

func (s *sotwService[U, D]) awaitingAck() bool {
	return s.awaiting != 0 && s.accepted < s.awaiting
}

func (s *sotwService[U, D]) push() {
	s.version++
	v := strconv.FormatUint(s.version, 10)
	resp := &discoveryv3.DiscoveryResponse{TypeUrl: string(s.typ), VersionInfo: v, Nonce: v}
	for _, r := range slices.SortedFunc(maps.Values(s.have), func(a, b Resource) int { return cmp.Compare(a.Name, b.Name) }) {
		// ponytail: packs every resource on every response; cache the Any per message if SotW pushes show in profiles.
		a, err := anypb.New(r.Message)
		if err != nil {
			s.fail(err)
			return
		}
		resp.Resources = append(resp.Resources, a)
	}
	s.awaiting = s.version
	s.answered = true
	s.send(resp)
}

// SotW has nothing like delta's initial resource versions, so a reconnect finds nothing stale to remove.
func (s *sotwService[U, D]) setDeferRemovals(func() bool) {}
func (s *sotwService[U, D]) deferredRemovals() bool       { return false }
func (s *sotwService[U, D]) sendDeferredRemovals()        {}
