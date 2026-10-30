package main

import (
	"maps"
	"net/netip"
	"slices"
	"sync"
)

// A service is a cluster: a name, and the endpoints it runs on.
type service struct {
	name      string
	endpoints []netip.AddrPort
}

// An update is the services before and after a change. Our real control plane sends what was added, changed and
// removed instead, which saves the ConfigBuilders work: an update is made once, but built for every Envoy.
type update struct {
	before, after map[string]service
}

// Merge keeps the oldest before and the newest after, so a burst of updates becomes one.
func (u update) Merge(next update) update { return update{u.before, next.after} }

// preRemove is the services until the remove round: those after, and those before that are yet to go.
func (u update) preRemove() map[string]service {
	m := map[string]service{}
	maps.Copy(m, u.before)
	maps.Copy(m, u.after)
	return m
}

// A clusterManager keeps the services, and tells each Envoy's stream when they change: it's our xds.Source.
type clusterManager struct {
	mu          sync.Mutex
	services    map[string]service // replaced, never changed, so updates can share it
	subscribers map[*subscriber]struct{}
}

type subscriber struct {
	deliver func(update)
}

func newClusterManager() *clusterManager {
	return &clusterManager{services: map[string]service{}, subscribers: map[*subscriber]struct{}{}}
}

// Subscribe is for xds.Server. Its first update has everything new.
func (m *clusterManager) Subscribe(_ int64, deliver func(update)) (update, func()) {
	sub := &subscriber{deliver}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.subscribers[sub] = struct{}{}
	return update{map[string]service{}, m.services}, func() {
		m.mu.Lock()
		defer m.mu.Unlock()
		delete(m.subscribers, sub)
	}
}

func (m *clusterManager) state() map[string]service {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.services
}

func (m *clusterManager) setEndpoints(name string, endpoints ...netip.AddrPort) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.set(name, endpoints)
}

func (m *clusterManager) addEndpoint(name string, endpoint netip.AddrPort) {
	m.mu.Lock()
	defer m.mu.Unlock()
	endpoints := m.services[name].endpoints
	if !slices.Contains(endpoints, endpoint) {
		endpoints = append(slices.Clone(endpoints), endpoint)
	}
	m.set(name, endpoints)
}

func (m *clusterManager) removeEndpoint(name string, endpoint netip.AddrPort) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if s, ok := m.services[name]; ok {
		m.set(name, slices.DeleteFunc(slices.Clone(s.endpoints), func(e netip.AddrPort) bool { return e == endpoint }))
	}
}

func (m *clusterManager) removeService(name string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.services[name]; ok {
		after := maps.Clone(m.services)
		delete(after, name)
		m.apply(after)
	}
}

func (m *clusterManager) set(name string, endpoints []netip.AddrPort) {
	after := maps.Clone(m.services)
	after[name] = service{name, endpoints}
	m.apply(after)
}

// apply publishes the new services. deliver never blocks, so we call it holding the lock, which keeps updates in
// order.
func (m *clusterManager) apply(after map[string]service) {
	u := update{m.services, after}
	m.services = after
	for sub := range m.subscribers {
		sub.deliver(u)
	}
}
