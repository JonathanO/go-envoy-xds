package xds

import (
	"testing"
	"testing/synctest"
	"time"

	corev3 "github.com/envoyproxy/go-control-plane/envoy/config/core/v3"
)

func TestResyncAfterNackCancelsTheResyncOfAStreamThatEnds(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		r := NewResyncAfterNack[string](time.Second, time.Second)
		resynced := false
		c := &Client[string]{Node: &corev3.Node{Id: "envoy"}, resync: func() { resynced = true }}
		r.Nacked(c, ClusterType, nil)
		r.Disconnected(c)
		time.Sleep(2 * time.Second)
		if resynced || len(r.pending) != 0 {
			t.Fatalf("resynced %v, pending %v", resynced, r.pending)
		}
	})
}
