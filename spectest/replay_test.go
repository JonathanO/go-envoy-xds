package spectest

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"maps"
	"os"
	"slices"
	"testing"
	"testing/synctest"

	"google.golang.org/genproto/googleapis/rpc/code"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
)

var replay = flag.Bool("replay", false, "replay vectors from stdin, as spec/docs/REPLAY.md says; see ./replay")

// TestReplay is replay mode, for the spec's differential fuzzer: it reads vectors from stdin, one per line, and prints
// what each Envoy got, one line per vector. It runs only with -replay: ./replay builds and runs it.
func TestReplay(t *testing.T) {
	if !*replay {
		t.Skip("replays vectors from stdin only with -replay")
	}
	in := bufio.NewScanner(os.Stdin)
	in.Buffer(nil, 64<<20)
	out := json.NewEncoder(os.Stdout)
	for in.Scan() {
		if len(in.Bytes()) == 0 {
			continue
		}
		var v vector
		if err := strict(in.Bytes(), &v); err != nil {
			t.Fatal(err)
		}
		runs := map[string]map[string]any{}
		for _, protocol := range v.Protocols {
			synctest.Test(t, func(t *testing.T) { runs[protocol] = replayVector(t, &v, protocol) })
		}
		if err := out.Encode(map[string]any{"name": v.Name, "runs": runs}); err != nil {
			t.Fatal(err)
		}
	}
	if err := in.Err(); err != nil {
		t.Fatal(err)
	}
}

// replayVector runs a vector's steps, recording what each Envoy got instead of checking it.
func replayVector(t *testing.T, v *vector, protocol string) map[string]any {
	steps := []any{}
	result := map[string]any{"steps": &steps}
	r, err := newRun(t.Context(), v, protocol)
	if err != nil {
		result["error"] = err.Error()
		return result
	}
	r.replaying = true
	for _, s := range v.Steps {
		if err := r.perform(s); err != nil {
			result["error"] = err.Error()
			return result
		}
		synctest.Wait() // every response to the step has been sent
		responses, ended, err := r.replayed()
		if err != nil {
			result["error"] = err.Error()
			return result
		}
		steps = append(steps, responses)
		if ended {
			return result
		}
	}
	return result
}

// replayed is what each Envoy got since the last step, and whether the Server ended any Envoy's stream.
func (r *run) replayed() ([]any, bool, error) {
	responses := []any{}
	ended := false
	for _, e := range r.envoys {
		c := e.conn
		if c == nil {
			continue
		}
		rs, err := c.take()
		if err != nil {
			return nil, false, fmt.Errorf("envoy %s: %w", e.name, err)
		}
		for _, rcv := range rs {
			resources := []json.RawMessage{}
			for _, name := range slices.Sorted(maps.Keys(rcv.resources)) {
				resources = append(resources, json.RawMessage(protojson.Format(rcv.resources[name])))
			}
			response := map[string]any{"envoy": e.name, "type": typeName(rcv.typ), "resources": resources}
			if r.delta {
				response["removed"] = append([]string{}, rcv.removed...)
			}
			responses = append(responses, response)
		}
		select {
		case err := <-c.ended:
			if c.closing && err == nil {
				e.conn = nil
				continue
			}
			responses = append(responses, map[string]any{"envoy": e.name, "ended": code.Code_name[int32(status.Code(err))]})
			ended = true
		default:
		}
	}
	return responses, ended, nil
}

// replayed replays a vector given as JSON, and gives the result as JSON with sorted keys.
func replayed(t *testing.T, vectorJSON string) string {
	t.Helper()
	var v vector
	if err := strict([]byte(vectorJSON), &v); err != nil {
		t.Fatal(err)
	}
	runs := map[string]map[string]any{}
	for _, protocol := range v.Protocols {
		synctest.Test(t, func(t *testing.T) { runs[protocol] = replayVector(t, &v, protocol) })
	}
	js, err := json.Marshal(map[string]any{"name": v.Name, "runs": runs})
	if err != nil {
		t.Fatal(err)
	}
	return string(js)
}

// canonical is JSON with sorted keys and no spaces, to compare with.
func canonical(t *testing.T, js string) string {
	t.Helper()
	var v any
	if err := json.Unmarshal([]byte(js), &v); err != nil {
		t.Fatal(err)
	}
	out, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	return string(out)
}

func TestReplayPrintsWhatEachEnvoyGotLeavingOutAnAckOfNoResponse(t *testing.T) {
	got := replayed(t, `{"name": "v", "protocols": ["sotw", "delta"], "serves": ["CDS"], "envoys": [{"name": "e", "view": "v"}], "steps": [
		{"update": [{"setView": "v", "groups": ["g"]}, {"editGroup": "g", "put": {"CDS": [{"name": "a"}]}}]},
		{"request": "CDS", "ack": "last", "expect": [{"type": "CDS", "resources": [{"name": "wrong"}]}]},
		{"request": "CDS", "ack": "last"},
		{"request": "CDS", "nack": "last-1"}]}`)

	want := `{"name": "v", "runs": {
		"sotw": {"steps": [[], [{"envoy": "e", "type": "CDS", "resources": [{"name": "a"}]}], [], []]},
		"delta": {"steps": [[], [{"envoy": "e", "type": "CDS", "resources": [{"name": "a"}], "removed": []}], [], []]}}}`
	if canonical(t, got) != canonical(t, want) {
		t.Errorf("got %s\nwant %s", canonical(t, got), canonical(t, want))
	}
}

func TestReplayStopsAtAStreamTheServerEnds(t *testing.T) {
	got := replayed(t, `{"name": "v", "protocols": ["delta"], "serves": ["CDS"], "envoys": [{"name": "e", "view": "v"}], "steps": [
		{"request": "CDS", "subscribe": ["a"], "unsubscribe": ["a"]},
		{"request": "CDS"}]}`)

	want := `{"name": "v", "runs": {"delta": {"steps": [[{"envoy": "e", "ended": "INVALID_ARGUMENT"}]]}}}`
	if canonical(t, got) != canonical(t, want) {
		t.Errorf("got %s\nwant %s", canonical(t, got), canonical(t, want))
	}
}

func TestReplayReportsAVectorInError(t *testing.T) {
	got := replayed(t, `{"name": "v", "protocols": ["sotw"], "serves": ["CDS"], "envoys": [{"name": "e", "view": "v"}], "steps": [
		{"disconnect": "e"}]}`)

	var result struct {
		Runs map[string]struct{ Error string }
	}
	if err := json.Unmarshal([]byte(got), &result); err != nil {
		t.Fatal(err)
	}
	if result.Runs["sotw"].Error == "" {
		t.Errorf("got %s, want an error", got)
	}
}
