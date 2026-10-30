package xds

import (
	"time"

	corev3 "github.com/envoyproxy/go-control-plane/envoy/config/core/v3"
	statuspb "google.golang.org/genproto/googleapis/rpc/status"
	"google.golang.org/protobuf/proto"
)

// A Resource is one xDS resource, under the name Envoy knows it by.
type Resource struct {
	Name    string
	Message proto.Message
}

// A Response is what a ConfigBuilder changes in one round of a push.
type Response struct {
	Updates []Resource // added or changed
	Removes []string   // names removed
}

func (r Response) empty() bool { return len(r.Updates) == 0 && len(r.Removes) == 0 }

// A ConfigBuilder turns state updates into the resources of one type.
//
// Each update is pushed in two rounds, every type's add round and then every type's remove round, each in the order
// that keeps Envoy's config whole (clusters before the routes that use them, say). So return what an update adds or
// changes from AddOrder, and what it removes from RemoveOrder: the Server makes it make before break. Remember a change
// can remove something too, e.g. a route that stops using a cluster.
//
// Return only what changed, ideally; returning more is safe, just more work. Return only names subscribed reports as
// subscribed to: Envoy rejects resources it didn't ask for.
//
// A Server calls a ConfigBuilder from each Envoy's stream, so from many goroutines at once, and never with two calls
// for one stream at the same time. A panic in one is logged, and ends that Envoy's stream with an error.
type ConfigBuilder[U, D any] interface {
	// Type is the type of resource it builds.
	Type() TypeURL
	// AddOrder is what update adds or changes.
	AddOrder(update U, subscribed func(name string) bool, details D) Response
	// RemoveOrder is what update removes.
	RemoveOrder(update U, subscribed func(name string) bool, details D) Response
	// ResourcesAddOrder is every subscribed resource during update's add round, for an Envoy subscribing then:
	// anything update adds, and anything it's yet to remove.
	ResourcesAddOrder(update U, subscribed func(name string) bool, details D) []Resource
	// ResourcesRemoveOrder is every subscribed resource once update is done.
	ResourcesRemoveOrder(update U, subscribed func(name string) bool, details D) []Resource
}

// A Source tells each connected Envoy about changes to its config.
type Source[U, D any] interface {
	// Subscribe starts delivering updates for the Envoy with these details, and returns the state they follow on
	// from. It must not miss an update published while it runs.
	//
	// The Source calls deliver with each update, in order, one at a time. deliver never blocks. After cancel returns,
	// deliver may still be called, and does nothing. The Server doesn't compare updates, so each one delivered is
	// pushed, ConfigBuilders and all, even if it's equal to the last.
	Subscribe(details D, deliver func(update U)) (current U, cancel func())
}

// A Merger is a state update that can take in the update after it, so an Envoy that's busy when several arrive gets
// them as one. Merge returns the update equal to applying the receiver, then next.
//
// A Server merges updates whose type is a Merger, and queues the rest. If the type also has a MergesWithLater() bool
// method, nothing later merges into an update for which it returns false: the Envoy gets that update, and ACKs it,
// first.
type Merger[U any] interface {
	Merge(next U) U
}

// A phaser is a Merger that sometimes mustn't be merged with what follows: the Envoy gets it, and ACKs it, first.
type phaser interface {
	MergesWithLater() bool
}

// EnvoyConfig is how a Server treats one Envoy.
type EnvoyConfig[D any] struct {
	// Details are passed to the ConfigBuilders and the Source, e.g. to pick the config this Envoy sees.
	Details D
	// DelayUpdatesUntilAckOf, if set, holds updates until this Envoy first ACKs the type, e.g. RouteType: Envoy ACKs
	// routes only once its clusters have initialised, so endpoint updates don't get in the way of that. See
	// https://github.com/envoyproxy/envoy/issues/16035. Only set it to a type the Envoy subscribes to. If
	// SubscribesTo leaves the type out, it's ignored; otherwise, with no ACK after 30 seconds, updates go anyway.
	DelayUpdatesUntilAckOf TypeURL
	// SubscribesTo is the types this Envoy gets from us, or nil for every type served. After a reconnect, a
	// resource Envoy kept is only removed once the types that may refer to it have been answered and ACKed, e.g. a
	// cluster after the route configs. Leave out types it doesn't use, e.g. RouteType for an Envoy with static
	// routes, or those removals wait for them, up to 30 seconds.
	SubscribesTo []TypeURL
}

// A Client is one Envoy's stream.
type Client[D any] struct {
	Node   *corev3.Node // from its first request
	Config EnvoyConfig[D]
	resync func()
}

// Resync closes the stream, so the Envoy reconnects and syncs from scratch: its first request on the new stream says
// exactly what config it has. Use it to unfreeze an Envoy that NACKed, once its config is fixed. Safe to call from any
// goroutine, at any time.
func (c *Client[D]) Resync() { c.resync() }

// Events are told what Envoys do. Any may be nil.
//
// They're called from the Envoy's stream, which waits for them, so they mustn't block for long.
type Events[D any] struct {
	// Connected is called when an Envoy's first request arrives.
	Connected func(c *Client[D])
	// Disconnected is called when the stream of a Connected Envoy ends.
	Disconnected func(c *Client[D])
	// Nacked is called when an Envoy rejects config it was sent. From then on, its stream is sent no more changes: it
	// keeps serving its last good config, which goes stale. Alert on it, and call Resync once the config is fixed, or
	// use ResyncAfterNack to retry on a backoff.
	Nacked func(c *Client[D], t TypeURL, detail *statuspb.Status)
}

// Metrics records how one stream's pushes go. A Server calls it from the stream's goroutine.
type Metrics interface {
	// PushBegan is called when an update starts going to Envoy.
	PushBegan()
	// PushCompleted is called when Envoy has ACKed all of the update.
	PushCompleted()
	// AwaitingAck is called when we start waiting for Envoy to ACK a type.
	AwaitingAck()
	// Acked is called when Envoy ACKs a type it was awaiting the ACK of.
	Acked()
	// Closed is called when the stream ends.
	Closed()
}

type noMetrics struct{}

func (noMetrics) PushBegan()     {}
func (noMetrics) PushCompleted() {}
func (noMetrics) AwaitingAck()   {}
func (noMetrics) Acked()         {}
func (noMetrics) Closed()        {}

// waitForAck is the longest we wait for an ACK of a type this Envoy may never subscribe to: before removing stale
// resources after a reconnect, and for DelayUpdatesUntilAckOf. Checked when something happens.
const waitForAck = 30 * time.Second
