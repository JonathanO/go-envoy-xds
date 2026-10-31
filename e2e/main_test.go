package main

import (
	"bufio"
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"

	clusterv3 "github.com/envoyproxy/go-control-plane/envoy/config/cluster/v3"
	corev3 "github.com/envoyproxy/go-control-plane/envoy/config/core/v3"
	discoveryv3 "github.com/envoyproxy/go-control-plane/envoy/service/discovery/v3"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/proto"

	xds "github.com/JonathanO/go-envoy-xds"
)

// The server says its ports, takes an update over its control API, serves it over ADS, and answers the rest of the
// API, as the harness uses it.
func TestServesWhatTheControlAPIGivesIt(t *testing.T) {
	in, hold := io.Pipe()
	defer hold.Close()
	outR, outW := io.Pipe()
	go func() { _ = run(in, outW) }()
	var started struct {
		ADS, Control int
		Deviations   map[string]string
	}
	line, err := bufio.NewReader(outR).ReadBytes('\n')
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(line, &started); err != nil {
		t.Fatal(err)
	}
	control := fmt.Sprintf("http://127.0.0.1:%d", started.Control)

	cluster, err := proto.Marshal(&clusterv3.Cluster{Name: "a"})
	if err != nil {
		t.Fatal(err)
	}
	post(t, control+"/update", http.StatusOK, fmt.Sprintf(`[{"setView": "envoy", "groups": ["g"]}, {"setGroup": "g", "resources": {"CDS": [%q]}}]`,
		base64.StdEncoding.EncodeToString(cluster)))
	post(t, control+"/update", http.StatusBadRequest, `[{"setGroup": "g", "resources": {"XDS": []}}]`)
	post(t, control+"/resync", http.StatusBadRequest, `{"node": "envoy"}`) // not connected yet

	conn, err := grpc.NewClient(fmt.Sprintf("127.0.0.1:%d", started.ADS), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	stream, err := discoveryv3.NewAggregatedDiscoveryServiceClient(conn).StreamAggregatedResources(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if err := stream.Send(&discoveryv3.DiscoveryRequest{Node: &corev3.Node{Id: "envoy"}, TypeUrl: string(xds.ClusterType)}); err != nil {
		t.Fatal(err)
	}
	resp, err := stream.Recv()
	if err != nil {
		t.Fatal(err)
	}
	if len(resp.GetResources()) != 1 {
		t.Fatalf("got %v, want cluster a", resp.GetResources())
	}
	var got clusterv3.Cluster
	if err := resp.GetResources()[0].UnmarshalTo(&got); err != nil || got.GetName() != "a" {
		t.Fatalf("got %v (%v), want cluster a", &got, err)
	}

	post(t, control+"/resync", http.StatusOK, `{"node": "envoy"}`)
	r, err := http.Get(control + "/idle")
	if err != nil {
		t.Fatal(err)
	}
	defer r.Body.Close()
	var idle map[string]bool
	if err := json.NewDecoder(r.Body).Decode(&idle); err != nil {
		t.Fatal(err)
	}
	if _, ok := idle["idle"]; !ok {
		t.Errorf("/idle answered %v", idle)
	}
	if started.Deviations["sotw/controlPlaneRestarts"] == "" {
		t.Errorf("deviations %v", started.Deviations)
	}
}

func post(t *testing.T, url string, want int, body string) {
	t.Helper()
	r, err := http.Post(url, "application/json", strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	defer r.Body.Close()
	var msg bytes.Buffer
	_, _ = msg.ReadFrom(r.Body)
	if r.StatusCode != want {
		t.Fatalf("%s %s: %d %s, want %d", url, body, r.StatusCode, msg.String(), want)
	}
}
