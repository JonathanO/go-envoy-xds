package xds_test

import (
	"slices"
	"testing"
	"testing/synctest"
	"time"

	discoveryv3 "github.com/envoyproxy/go-control-plane/envoy/service/discovery/v3"
	statuspb "google.golang.org/genproto/googleapis/rpc/status"

	xds "github.com/JonathanO/go-envoy-xds"
)

func TestResyncAfterNackBacksOffWhileNacksRepeat(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		r := xds.NewResyncAfterNack[string](time.Second, 4*time.Second)
		srv := newServer(t, newSource(cluster("a")), xds.EnvoyConfig[string]{}, xds.Events[string]{Nacked: r.Nacked, Disconnected: r.Disconnected})
		var waits []time.Duration
		for range 4 {
			e := startSotw(t, srv)
			cds := e.request(xds.ClusterType, nil)[0]
			nacked := time.Now()
			e.send(&discoveryv3.DiscoveryRequest{Node: node, TypeUrl: string(xds.ClusterType), ResponseNonce: cds.GetNonce(),
				ErrorDetail: &statuspb.Status{Message: "no"}})
			e.send(&discoveryv3.DiscoveryRequest{Node: node, TypeUrl: string(xds.ClusterType), ResponseNonce: cds.GetNonce(),
				ErrorDetail: &statuspb.Status{Message: "again"}}) // only a stream's first NACK counts
			if err := <-e.ended; err != nil {
				t.Fatal(err)
			}
			waits = append(waits, time.Since(nacked))
		}
		if want := []time.Duration{time.Second, 2 * time.Second, 4 * time.Second, 4 * time.Second}; !slices.Equal(waits, want) {
			t.Fatalf("waited %v, want %v", waits, want)
		}

		time.Sleep(9 * time.Second) // quiet for over twice the max
		e := startSotw(t, srv)
		cds := e.request(xds.ClusterType, nil)[0]
		nacked := time.Now()
		e.send(&discoveryv3.DiscoveryRequest{Node: node, TypeUrl: string(xds.ClusterType), ResponseNonce: cds.GetNonce(),
			ErrorDetail: &statuspb.Status{Message: "no"}})
		<-e.ended
		if waited := time.Since(nacked); waited != time.Second {
			t.Fatalf("waited %v after a quiet spell, want to start again from 1s", waited)
		}
	})
}
