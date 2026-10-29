package xds

import (
	"fmt"
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

func TestResyncAfterNackForgetsEnvoysThatWentQuiet(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		r := NewResyncAfterNack[string](100*time.Millisecond, 200*time.Millisecond)
		nack := func(id string) {
			r.Nacked(&Client[string]{Node: &corev3.Node{Id: id}, resync: func() {}}, ClusterType, nil)
		}
		for i := range 100 {
			nack(fmt.Sprint("pod-", i))
		}
		time.Sleep(401 * time.Millisecond) // over twice the max
		nack("envoy")
		if len(r.backoffs) != 1 {
			t.Fatalf("tracking %d nodes", len(r.backoffs))
		}
	})
}
