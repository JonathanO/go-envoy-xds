package xds

import (
	"cmp"
	"maps"
	"slices"

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

	nonce    string // of the last response, which is also its version
	awaiting bool   // for Envoy to ACK or NACK it
}

func (s *sotwService[U, D]) process(req *discoveryv3.DiscoveryRequest) error {
	s.active = true
	if s.nonce != "" {
		if req.GetResponseNonce() != s.nonce {
			s.log.Debug("Ignoring a request answering an earlier response", "type", s.typ, "nonce", req.GetResponseNonce())
			return nil
		}
		// An ACK names the version it accepts, and a NACK has error detail: the manager sends no more changes after
		// one. Anything else answers nothing.
		if req.GetVersionInfo() == s.nonce || req.GetErrorDetail() != nil {
			s.awaiting = false
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
		s.apply(s.adds(s.subs.subscribed))
	case removing:
		s.apply(s.removes(s.subs.subscribed))
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
	if s.apply(s.adds(s.subs.subscribed)) {
		s.push()
	}
}

func (s *sotwService[U, D]) sendRemoves() {
	if s.apply(s.removes(s.subs.subscribed)) {
		s.push()
	}
}

func (s *sotwService[U, D]) awaitingAck() bool { return s.awaiting }

func (s *sotwService[U, D]) push() {
	v := s.nextVersion()
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
	s.nonce, s.awaiting = v, true
	s.send(resp)
}

// SotW has nothing like delta's initial resource versions, so a reconnect finds nothing stale to remove: deferred is
// always empty.
func (s *sotwService[U, D]) setDeferRemovals(func() bool) {}
func (s *sotwService[U, D]) sendDeferredRemovals()        {}
