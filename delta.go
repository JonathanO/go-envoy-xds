package xds

import (
	"maps"
	"slices"
	"strconv"

	discoveryv3 "github.com/envoyproxy/go-control-plane/envoy/service/discovery/v3"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
)

// deltaService serves one type over delta xDS, where each response is only what changed.
type deltaService[U, D any] struct {
	base[U, D]
	send func(*discoveryv3.DeltaDiscoveryResponse)
	fail func(error)
	subs deltaSubs

	version       uint64 // of the last response; also its nonce
	unanswered    string // nonce of the response Envoy hasn't answered, or ""
	deferred      set    // stale names Envoy kept over a reconnect, to remove in remove order
	deferRemovals func() bool
	// What subscriptions were answered with while the staged change waited its turn, so it needn't send them again.
	answeredDuringChange map[string]proto.Message
}

func (s *deltaService[U, D]) process(req *discoveryv3.DeltaDiscoveryRequest) error {
	s.active = true
	// An ACK, or a NACK if it has error detail: either way, answered. The manager sends no more changes after a NACK.
	if n := req.GetResponseNonce(); n != "" && n == s.unanswered {
		s.unanswered = ""
	}
	// Envoy puts subscription changes it has queued into its ACKs, so one request can do both.
	added, changed, err := s.subs.update(req.GetResourceNamesSubscribe(), req.GetResourceNamesUnsubscribe())
	if err != nil {
		return err
	}
	if changed {
		s.subscriptionChanged(req.GetResourceNamesSubscribe(), added, req.GetInitialResourceVersions())
	}
	return nil
}

func (s *deltaService[U, D]) subscriptionChanged(names []string, added func(string) bool, kept map[string]string) {
	// Envoy wants everything it just subscribed to, or to hear that it doesn't exist. Some names, like the wildcard,
	// aren't resources: added says which are.
	removed := set{}
	for _, n := range names {
		if _, ok := kept[n]; !ok && added(n) {
			removed[n] = struct{}{}
		}
	}
	// A reconnecting Envoy says what it kept, which may include what's gone since: it must hear those are gone, if
	// it's subscribed to them, maybe by wildcard rather than by name.
	stale := set{}
	for n := range kept {
		if added(n) {
			stale[n] = struct{}{}
		}
	}
	rs := s.resources(added)
	for _, r := range rs {
		delete(removed, r.Name)
		delete(stale, r.Name)
		delete(s.deferred, r.Name)
	}
	if s.round == adding {
		for _, r := range rs {
			s.answeredDuringChange[r.Name] = r.Message
		}
	}
	if s.deferRemovals() {
		maps.Copy(s.deferred, stale)
	} else {
		maps.Copy(removed, stale)
	}
	s.push(rs, slices.Sorted(maps.Keys(removed)))
}

func (s *deltaService[U, D]) sendAdds() {
	if r, ok := s.adds(s.subs.subscribed); ok {
		s.pushChange(r)
	}
}

func (s *deltaService[U, D]) sendRemoves() {
	if r, ok := s.removes(s.subs.subscribed); ok {
		s.pushChange(r)
	}
}

func (s *deltaService[U, D]) pushChange(r Response) {
	updates := slices.DeleteFunc(slices.Clone(r.Updates), func(u Resource) bool {
		return same(u.Message, s.answeredDuringChange[u.Name])
	})
	clear(s.answeredDuringChange)
	if len(updates) > 0 || len(r.Removes) > 0 {
		s.push(updates, r.Removes)
	}
}

func (s *deltaService[U, D]) awaitingAck() bool { return s.unanswered != "" }

func (s *deltaService[U, D]) setDeferRemovals(f func() bool) { s.deferRemovals = f }
func (s *deltaService[U, D]) deferredRemovals() bool         { return len(s.deferred) > 0 }

func (s *deltaService[U, D]) sendDeferredRemovals() {
	removals := set{}
	for n := range s.deferred {
		if s.subs.subscribed(n) {
			removals[n] = struct{}{}
		}
	}
	for _, r := range s.resources(s.subs.subscribed) {
		delete(removals, r.Name)
	}
	clear(s.deferred)
	if len(removals) > 0 {
		s.push(nil, slices.Sorted(maps.Keys(removals)))
	}
}

func (s *deltaService[U, D]) push(rs []Resource, removes []string) {
	s.version++
	v := strconv.FormatUint(s.version, 10)
	resp := &discoveryv3.DeltaDiscoveryResponse{TypeUrl: string(s.typ), SystemVersionInfo: v, Nonce: v, RemovedResources: removes}
	for _, r := range rs {
		a, err := anypb.New(r.Message)
		if err != nil {
			s.fail(err)
			return
		}
		resp.Resources = append(resp.Resources, &discoveryv3.Resource{Name: r.Name, Version: v, Resource: a})
	}
	s.unanswered = v
	s.answered = true
	s.send(resp)
}
