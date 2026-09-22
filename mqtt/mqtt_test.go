package mqtt

// Own-both-ends validation of the MQTT CHANNEL adapter. We stand up the
// broker (broker.go), then prove two layers against it:
//
//   - the dialect (client.go): QoS 1 with PUBACK, retain, wildcards, last will,
//     persistent sessions — the app routing the transports manifest names;
//   - the relay (relay.go): a far end and a host that both dial OUT, an
//     envelope naming a CALL or a CHANNEL atom, fired through a witness stub
//     that stamps X-8-Witness, answered by a retained receipt; plus the
//     store-and-forward, idempotency, policy (both directions) and
//     no-credential guarantees.
//
// Set MQTT_TEST_BROKER=mqtt://host:1883 to run the relay round-trip against a
// real broker (mosquitto) as well.

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/rrrishi123/adapters/internal/httpx"
)

// --- the July harness tests, unchanged: subscribe=CHANNEL, publish=CALL ---

func TestMQTT_Loopback_PublishReachesSubscriber(t *testing.T) {
	br, err := NewBroker()
	if err != nil {
		t.Fatalf("broker: %v", err)
	}
	defer br.Close()

	sub, err := Dial(br.Addr(), "sub")
	if err != nil {
		t.Fatalf("subscriber dial: %v", err)
	}
	defer sub.Close()
	frames, err := sub.Subscribe("wire/probe")
	if err != nil {
		t.Fatalf("subscribe (open channel): %v", err)
	}

	pub, err := Dial(br.Addr(), "pub")
	if err != nil {
		t.Fatalf("publisher dial: %v", err)
	}
	defer pub.Close()
	if err := pub.Publish("wire/probe", []byte(`{"mode":"CHANNEL via MQTT","ok":true}`)); err != nil {
		t.Fatalf("publish (call): %v", err)
	}

	select {
	case m := <-frames:
		if !strings.Contains(string(m), `"ok":true`) {
			t.Fatalf("channel frame mismatch: %s", m)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("publish -> subscribe round-trip failed: nothing arrived on the CHANNEL")
	}
}

func TestMQTT_Loopback_TopicRouting(t *testing.T) {
	br, err := NewBroker()
	if err != nil {
		t.Fatalf("broker: %v", err)
	}
	defer br.Close()

	sub, err := Dial(br.Addr(), "sub")
	if err != nil {
		t.Fatalf("subscriber dial: %v", err)
	}
	defer sub.Close()
	frames, err := sub.Subscribe("wire/a")
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}

	pub, err := Dial(br.Addr(), "pub")
	if err != nil {
		t.Fatalf("publisher dial: %v", err)
	}
	defer pub.Close()
	if err := pub.Publish("wire/b", []byte("should-not-arrive")); err != nil {
		t.Fatalf("publish: %v", err)
	}

	select {
	case m := <-frames:
		t.Fatalf("message leaked across topics: %s", m)
	case <-time.After(300 * time.Millisecond):
	}
}

// --- the dialect: QoS 1, retain, wildcards, will, persistent sessions ---

func TestMQTT_TopicMatching(t *testing.T) {
	cases := []struct {
		filter, topic string
		want          bool
	}{
		{"wire/commands/+", "wire/commands/abc", true},
		{"wire/commands/+", "wire/commands/abc/def", false},
		{"wire/#", "wire/commands/abc/def", true},
		{"wire/#", "wire", true},
		{"+/receipts/x", "wire/receipts/x", true},
		{"wire/receipts/x", "wire/receipts/y", false},
		{"#", "$SYS/broker", false},
		{"$SYS/#", "$SYS/broker", true},
	}
	for _, c := range cases {
		if got := MatchTopic(c.filter, c.topic); got != c.want {
			t.Errorf("MatchTopic(%q, %q) = %v, want %v", c.filter, c.topic, got, c.want)
		}
	}
	for _, bad := range []string{"", "a/#/b", "a+/b", "#a"} {
		if ValidFilter(bad) {
			t.Errorf("ValidFilter(%q) should be false", bad)
		}
	}
	if ValidTopic("a/+") || ValidTopic("") || !ValidTopic("wire/commands/x") {
		t.Error("ValidTopic misjudged")
	}
}

func TestMQTT_QoS1_Retain_Wildcard(t *testing.T) {
	br, err := NewBroker()
	if err != nil {
		t.Fatal(err)
	}
	defer br.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	pub, err := Connect(ctx, Options{URL: br.URL(), ClientID: "pub", CleanSession: true})
	if err != nil {
		t.Fatal(err)
	}
	defer pub.Close()
	// QoS 1: returns only once the broker PUBACKed; retained for late subscribers
	if err := pub.PublishMessage(ctx, Message{Topic: "wire/receipts/r1", Payload: []byte("kept"), QoS: 1, Retain: true}); err != nil {
		t.Fatalf("publish qos1 retain: %v", err)
	}
	if m, ok := br.Retained("wire/receipts/r1"); !ok || string(m.Payload) != "kept" {
		t.Fatalf("broker did not retain: %v %q", ok, m.Payload)
	}

	// a LATE subscriber on a wildcard gets the retained message, flagged retain
	sub, err := Connect(ctx, Options{URL: br.URL(), ClientID: "sub", CleanSession: true})
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Close()
	ch, err := sub.SubscribeMessages(ctx, "wire/receipts/+", 1)
	if err != nil {
		t.Fatal(err)
	}
	select {
	case m := <-ch:
		if !m.Retain || m.Topic != "wire/receipts/r1" || string(m.Payload) != "kept" || m.QoS != 1 {
			t.Fatalf("retained delivery wrong: %+v", m)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("late subscriber never got the retained message")
	}
	// a live QoS 1 publish arrives with retain cleared
	if err := pub.PublishMessage(ctx, Message{Topic: "wire/receipts/r2", Payload: []byte("live"), QoS: 1}); err != nil {
		t.Fatal(err)
	}
	select {
	case m := <-ch:
		if m.Retain || m.Topic != "wire/receipts/r2" {
			t.Fatalf("live delivery wrong: %+v", m)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("live message never arrived")
	}
	// clearing: an empty retained payload removes it
	if err := pub.PublishMessage(ctx, Message{Topic: "wire/receipts/r1", QoS: 1, Retain: true}); err != nil {
		t.Fatal(err)
	}
	if _, ok := br.Retained("wire/receipts/r1"); ok {
		t.Fatal("empty retained payload did not clear the topic")
	}
	if err := pub.PublishMessage(ctx, Message{Topic: "x", QoS: 2}); err != ErrQoS2 {
		t.Fatalf("QoS 2 must be refused, got %v", err)
	}
}

// A persistent session (CleanSession=false) keeps the subscription and queues
// QoS 1 messages while the client is away — the "store" in store-and-forward.
func TestMQTT_PersistentSession_storeAndForward(t *testing.T) {
	br, err := NewBroker()
	if err != nil {
		t.Fatal(err)
	}
	defer br.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	host, err := Connect(ctx, Options{URL: br.URL(), ClientID: "host", CleanSession: false})
	if err != nil {
		t.Fatal(err)
	}
	if host.SessionPresent() {
		t.Fatal("first connect must not report a session present")
	}
	if _, err := host.SubscribeMessages(ctx, "wire/commands/+", 1); err != nil {
		t.Fatal(err)
	}
	host.Close() // graceful DISCONNECT: session survives

	far, err := Connect(ctx, Options{URL: br.URL(), ClientID: "far", CleanSession: true})
	if err != nil {
		t.Fatal(err)
	}
	defer far.Close()
	if err := far.PublishMessage(ctx, Message{Topic: "wire/commands/c1", Payload: []byte("while-away"), QoS: 1}); err != nil {
		t.Fatal(err)
	}

	host2, err := Connect(ctx, Options{URL: br.URL(), ClientID: "host", CleanSession: false})
	if err != nil {
		t.Fatal(err)
	}
	defer host2.Close()
	if !host2.SessionPresent() {
		t.Fatal("broker did not resume the persistent session")
	}
	ch, err := host2.SubscribeMessages(ctx, "wire/commands/+", 1) // re-subscribe is harmless; backlog was flushed at CONNACK
	if err != nil {
		t.Fatal(err)
	}
	select {
	case m := <-ch:
		if string(m.Payload) != "while-away" || !m.Dup {
			t.Fatalf("queued delivery wrong: %+v", m)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("the message published while the host was away never arrived")
	}
}

// A client that vanishes without DISCONNECT has its will published; one that
// leaves gracefully does not.
func TestMQTT_LastWill(t *testing.T) {
	br, err := NewBroker()
	if err != nil {
		t.Fatal(err)
	}
	defer br.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	watcher, err := Connect(ctx, Options{URL: br.URL(), ClientID: "watcher", CleanSession: true})
	if err != nil {
		t.Fatal(err)
	}
	defer watcher.Close()
	ch, err := watcher.SubscribeMessages(ctx, "wire/presence/+", 1)
	if err != nil {
		t.Fatal(err)
	}

	will := Message{Topic: "wire/presence/node1", Payload: []byte("offline"), QoS: 1, Retain: true}
	node, err := Connect(ctx, Options{URL: br.URL(), ClientID: "node1", CleanSession: true, Will: &will})
	if err != nil {
		t.Fatal(err)
	}
	node.conn.Close() // vanish: no DISCONNECT
	select {
	case m := <-ch:
		if m.Topic != will.Topic || string(m.Payload) != "offline" {
			t.Fatalf("will wrong: %+v", m)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("will never published")
	}
	if _, ok := br.Retained(will.Topic); !ok {
		t.Fatal("retained will not stored")
	}

	node2, err := Connect(ctx, Options{URL: br.URL(), ClientID: "node2", CleanSession: true, Will: &Message{Topic: "wire/presence/node2", Payload: []byte("offline")}})
	if err != nil {
		t.Fatal(err)
	}
	node2.Close() // graceful
	select {
	case m := <-ch:
		t.Fatalf("will published after a graceful DISCONNECT: %+v", m)
	case <-time.After(300 * time.Millisecond):
	}
}

func TestMQTT_BrokerAuth(t *testing.T) {
	br, err := NewBroker()
	if err != nil {
		t.Fatal(err)
	}
	defer br.Close()
	br.Auth = func(u, p string) bool { return u == "node" && p == "s3cret" }
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := Connect(ctx, Options{URL: br.URL(), ClientID: "x", CleanSession: true}); err == nil || !strings.Contains(err.Error(), "bad username") {
		t.Fatalf("anonymous connect should be refused rc=4, got %v", err)
	}
	c, err := Connect(ctx, Options{URL: br.URL(), ClientID: "x", CleanSession: true, Username: "node", Password: "s3cret"})
	if err != nil {
		t.Fatalf("authenticated connect: %v", err)
	}
	c.Close()
}

// --- the relay: both nodes dial OUT; envelope in → witnessed fire → retained receipt out ---

type relayRig struct {
	broker  *Broker
	url     string
	target  *httptest.Server
	witness *httptest.Server
	cfg     Config
}

func newRig(t *testing.T) *relayRig {
	t.Helper()
	r := &relayRig{}
	if ext := os.Getenv("MQTT_TEST_BROKER"); ext != "" {
		r.url = ext
	} else {
		br, err := NewBroker()
		if err != nil {
			t.Fatal(err)
		}
		r.broker = br
		r.url = br.URL()
		t.Cleanup(func() { br.Close() })
	}
	r.target = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		if req.Header.Get("Authorization") != "" {
			fmt.Fprint(w, `{"transport":"mqtt","ok":true,"auth":true}`) // the stub forwards bodies, not target headers
			return
		}
		fmt.Fprint(w, `{"transport":"mqtt","ok":true}`)
	}))
	t.Cleanup(r.target.Close)
	r.witness = httptest.NewServer(WitnessStub())
	t.Cleanup(r.witness.Close)
	// a unique prefix per test so a real broker's retained state cannot bleed across runs
	r.cfg = Config{Broker: r.url, Prefix: fmt.Sprintf("wire-test/%d", time.Now().UnixNano()), Collector: r.witness.URL, KeepAlive: 5 * time.Second}
	return r
}

func (r *relayRig) host(t *testing.T, node string, policy Policy) (*Host, context.CancelFunc, <-chan error) {
	t.Helper()
	cfg := r.cfg
	cfg.Node, cfg.Actor = node, node+"-actor"
	h := &Host{Config: cfg, Policy: policy, Log: log.New(os.Stderr, "", 0)}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- h.Serve(ctx) }()
	select {
	case <-h.Ready():
	case err := <-done:
		t.Fatalf("host ended before ready: %v", err)
	case <-time.After(5 * time.Second):
		t.Fatal("host never became ready")
	}
	return h, cancel, done
}

func (r *relayRig) far(node string) *FarEnd {
	cfg := r.cfg
	cfg.Node = node
	return &FarEnd{Config: cfg}
}

func TestMQTT_Relay_CALL_roundTrip(t *testing.T) {
	r := newRig(t)
	h, stop, done := r.host(t, "host-a", nil)
	defer func() { stop(); <-done }()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	env := &Envelope{ULID: "call-1", Seq: 1, Atom: AtomCall, Call: &httpx.Request{Method: "GET", URL: r.target.URL + "/ping"}, Why: "test"}
	rec, err := r.far("sandbox").Fire(ctx, env)
	if err != nil {
		t.Fatalf("fire: %v", err)
	}
	if !rec.Decision.Fired || rec.Status != 200 || !strings.Contains(rec.Body, `"ok":true`) {
		t.Fatalf("thin receipt: %+v", rec)
	}
	if rec.Witness.Line == "" || !strings.Contains(rec.Witness.Line, "by host-a-actor") || rec.Witness.LedgerID == "" {
		t.Fatalf("receipt lacks the reafference: %+v", rec.Witness)
	}
	if rec.Atom != AtomCall || rec.Call == nil || rec.Call.URL != r.target.URL+"/ping" || rec.Agent != "sandbox" || rec.Why != "test" {
		t.Fatalf("echo wrong: %+v", rec)
	}
	if rec.Route.Topic != r.cfg.Topic("receipts", "call-1") || rec.Route.QoS != 1 || !rec.Route.Retain {
		t.Fatalf("route wrong: %+v", rec.Route)
	}
	if rec.BodyDigest != Digest([]byte(`{"transport":"mqtt","ok":true}`)) {
		t.Fatal("digest mismatch")
	}
	if r.broker != nil {
		if _, ok := r.broker.Retained(rec.Route.Topic); !ok {
			t.Fatal("receipt not retained at the broker")
		}
	}
	// a second Fire of the same ULID returns the retained receipt without re-proposing
	rec2, err := r.far("sandbox").Fire(ctx, &Envelope{ULID: "call-1", Seq: 1, Atom: AtomCall, Call: &httpx.Request{URL: r.target.URL + "/ping"}})
	if err != nil {
		t.Fatal(err)
	}
	if rec2.Witness.LedgerID != rec.Witness.LedgerID {
		t.Fatalf("duplicate ULID fired again: %s vs %s", rec2.Witness.LedgerID, rec.Witness.LedgerID)
	}
	if st := h.Stats(); st.Fired != 1 {
		t.Fatalf("host fired %d, want 1: %+v", st.Fired, st)
	}
	// presence: both nodes announced themselves (retained)
	if r.broker != nil {
		for _, n := range []string{"host-a", "sandbox"} {
			m, ok := r.broker.Retained(r.cfg.PresenceTopic(n))
			if !ok {
				t.Fatalf("no retained presence for %s", n)
			}
			var p Presence
			_ = json.Unmarshal(m.Payload, &p)
			if p.Node != n || p.State == "" {
				t.Fatalf("presence wrong for %s: %s", n, m.Payload)
			}
		}
	}
}

func TestMQTT_Relay_CHANNEL_roundTrip(t *testing.T) {
	r := newRig(t)
	_, stop, done := r.host(t, "host-b", nil)
	defer func() { stop(); <-done }()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	env := &Envelope{ULID: "chan-1", Seq: 2, Atom: AtomChannel, Channel: &ChannelOp{Session: "fox", Method: "browsingContext.getTree", Params: json.RawMessage(`{"maxDepth":1}`)}}
	rec, err := r.far("sandbox").Fire(ctx, env)
	if err != nil {
		t.Fatalf("fire: %v", err)
	}
	if !rec.Decision.Fired || rec.Status != 200 || rec.Atom != AtomChannel || rec.Channel == nil || rec.Channel.Session != "fox" {
		t.Fatalf("thin receipt: %+v", rec)
	}
	if !strings.Contains(rec.Witness.Line, "channel") || !strings.Contains(rec.Witness.Line, "@fox") || rec.Witness.Ledger == "" {
		t.Fatalf("witness line wrong: %+v", rec.Witness)
	}
	var out struct {
		ID     int `json:"id"`
		Result struct {
			Echo   string          `json:"echo"`
			Params json.RawMessage `json:"params"`
		} `json:"result"`
	}
	if err := json.Unmarshal([]byte(rec.Body), &out); err != nil || out.ID != 1 || out.Result.Echo != "browsingContext.getTree" || !strings.Contains(string(out.Result.Params), "maxDepth") {
		t.Fatalf("command result not carried: %s (%v)", rec.Body, err)
	}
}

// The relay's reason to exist: a command published while the host is DOWN is
// delivered when it returns (persistent session), and the receipt is waiting
// (retained) for a far end that also went away.
func TestMQTT_Relay_HostOffline_storeAndForward(t *testing.T) {
	r := newRig(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	// 1. the host has been online once (its persistent session exists), then goes down
	h, stop, done := r.host(t, "host-c", nil)
	stop()
	if err := <-done; err != nil {
		t.Fatalf("host serve: %v", err)
	}

	// 2. the far end proposes while the host is away, then leaves too
	far := r.far("sandbox")
	c, err := far.Connect(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := far.Propose(ctx, c, &Envelope{ULID: "late-1", Seq: 3, Atom: AtomCall, Call: &httpx.Request{URL: r.target.URL + "/late"}}); err != nil {
		t.Fatal(err)
	}
	c.Close()

	// 3. the host returns: the broker hands it the queued command; it fires and answers
	hctx, hcancel := context.WithCancel(context.Background())
	hdone := make(chan error, 1)
	go func() { hdone <- h.Serve(hctx) }()
	defer func() { hcancel(); <-hdone }()
	deadline := time.Now().Add(5 * time.Second)
	for h.Stats().Fired < 1 && time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
	}
	if st := h.Stats(); st.Fired != 1 || st.Sessions != 2 {
		t.Fatalf("host did not process the queued command: %+v", st)
	}

	// 4. the far end comes back and finds the retained receipt
	c2, err := far.Connect(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer c2.Close()
	rec, err := far.Await(ctx, c2, "late-1")
	if err != nil {
		t.Fatal(err)
	}
	if !rec.Decision.Fired || rec.Status != 200 || rec.Witness.Line == "" {
		t.Fatalf("retained receipt thin: %+v", rec)
	}
}

// The host is idempotent by ULID even across sessions: a redelivered command
// whose receipt is already retained is skipped, never fired twice.
func TestMQTT_Relay_Idempotent_acrossSessions(t *testing.T) {
	r := newRig(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	h, stop, done := r.host(t, "host-d", nil)
	if _, err := r.far("sandbox").Fire(ctx, &Envelope{ULID: "once", Seq: 1, Atom: AtomCall, Call: &httpx.Request{URL: r.target.URL}}); err != nil {
		t.Fatal(err)
	}
	stop()
	<-done

	// a fresh Host value (no in-memory seen set) — only the broker's retained receipts tell it
	h2 := &Host{Config: h.Config}
	hctx, hcancel := context.WithCancel(context.Background())
	hdone := make(chan error, 1)
	go func() { hdone <- h2.Serve(hctx) }()
	defer func() { hcancel(); <-hdone }()
	<-h2.Ready()

	far := r.far("sandbox")
	c, err := far.Connect(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	if err := far.Propose(ctx, c, &Envelope{ULID: "once", Seq: 1, Atom: AtomCall, Call: &httpx.Request{URL: r.target.URL}}); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(3 * time.Second)
	for h2.Stats().Commands < 1 && time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
	}
	if st := h2.Stats(); st.Commands != 1 || st.Skipped != 1 || st.Fired != 0 {
		t.Fatalf("duplicate not skipped: %+v", st)
	}
}

// The far end only proposes: the host's policy decides what fires and what
// leaves in the receipt; a credential in an envelope never reaches the broker.
func TestMQTT_Relay_PolicyGatesBothWays(t *testing.T) {
	r := newRig(t)
	pf := t.TempDir() + "/policy.json"
	if err := os.WriteFile(pf, []byte(`{"allow_atoms":["call"],"allow_url_prefixes":["`+r.target.URL+`"],"allow_methods":["GET"],"egress":{"body":false,"headers":false}}`), 0o644); err != nil {
		t.Fatal(err)
	}
	_, stop, done := r.host(t, "host-e", FilePolicy{Path: pf})
	defer func() { stop(); <-done }()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	far := r.far("sandbox")

	// allowed, but egress-gated: digest and length only, no body, no headers
	rec, err := far.Fire(ctx, &Envelope{ULID: "p-ok", Seq: 1, Atom: AtomCall, Call: &httpx.Request{Method: "GET", URL: r.target.URL + "/x"}})
	if err != nil {
		t.Fatal(err)
	}
	if !rec.Decision.Fired || rec.Status != 200 || rec.Body != "" || rec.Headers != nil || rec.BodyLen == 0 || rec.BodyDigest == "" || rec.Witness.Line == "" {
		t.Fatalf("egress gate not applied: %+v", rec)
	}
	if rec.Decision.Egress == nil || rec.Decision.Egress.Body {
		t.Fatalf("decision does not record the egress gate: %+v", rec.Decision)
	}
	// refused: wrong method
	rec, err = far.Fire(ctx, &Envelope{ULID: "p-post", Seq: 2, Atom: AtomCall, Call: &httpx.Request{Method: "POST", URL: r.target.URL + "/x"}})
	if err != nil {
		t.Fatal(err)
	}
	if rec.Decision.Fired || rec.Decision.Policy != "file-policy" || !strings.Contains(rec.Decision.Reason, "allow_methods") {
		t.Fatalf("POST should be refused: %+v", rec.Decision)
	}
	// refused: atom not allowed
	rec, err = far.Fire(ctx, &Envelope{ULID: "p-chan", Seq: 3, Atom: AtomChannel, Channel: &ChannelOp{Session: "fox", Method: "x"}})
	if err != nil {
		t.Fatal(err)
	}
	if rec.Decision.Fired || !strings.Contains(rec.Decision.Reason, "allow_atoms") {
		t.Fatalf("channel should be refused: %+v", rec.Decision)
	}
	// a credential never leaves the far end
	err = far.Propose(ctx, nil, &Envelope{ULID: "p-auth", Seq: 4, Atom: AtomCall, Call: &httpx.Request{URL: r.target.URL, Headers: map[string]string{"Authorization": "Bearer x"}}})
	if err == nil || !strings.Contains(err.Error(), "Authorization") {
		t.Fatalf("credential header must be rejected before publish, got %v", err)
	}
	// malformed policy = closed
	if err := os.WriteFile(pf, []byte(`{not json`), 0o644); err != nil {
		t.Fatal(err)
	}
	rec, err = far.Fire(ctx, &Envelope{ULID: "p-closed", Seq: 5, Atom: AtomCall, Call: &httpx.Request{URL: r.target.URL}})
	if err != nil {
		t.Fatal(err)
	}
	if rec.Decision.Fired || !strings.Contains(rec.Decision.Reason, "malformed") {
		t.Fatalf("malformed policy must fail closed: %+v", rec.Decision)
	}
}

// auth_slot: the far end names a slot; the host resolves it in memory and the
// receipt never echoes the credential.
func TestMQTT_Relay_AuthSlot(t *testing.T) {
	r := newRig(t)
	slots := t.TempDir() + "/slots.json"
	if err := os.WriteFile(slots, []byte(`{"lab":{"type":"bearer","key":"never-published"}}`), 0o600); err != nil {
		t.Fatal(err)
	}
	r.cfg.Slots = slots
	_, stop, done := r.host(t, "host-f", nil)
	defer func() { stop(); <-done }()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	rec, err := r.far("sandbox").Fire(ctx, &Envelope{ULID: "slot-1", Seq: 1, Atom: AtomCall, Call: &httpx.Request{URL: r.target.URL}, AuthSlot: "lab"})
	if err != nil {
		t.Fatal(err)
	}
	if !rec.Decision.Fired || !strings.Contains(rec.Body, `"auth":true`) {
		t.Fatalf("slot not applied at the host: %+v", rec)
	}
	raw, _ := json.Marshal(rec)
	if strings.Contains(string(raw), "never-published") {
		t.Fatal("credential leaked into the receipt")
	}
	rec, err = r.far("sandbox").Fire(ctx, &Envelope{ULID: "slot-2", Seq: 2, Atom: AtomCall, Call: &httpx.Request{URL: r.target.URL}, AuthSlot: "nope"})
	if err != nil {
		t.Fatal(err)
	}
	if rec.Decision.Fired || rec.Decision.Policy != "auth-slot" {
		t.Fatalf("unknown slot must refuse: %+v", rec.Decision)
	}
}

// Every path yields a receipt: an unparseable payload, a mismatched ULID and
// an expired proposal are answered, never dropped.
func TestMQTT_Relay_EveryPathYieldsReceipt(t *testing.T) {
	h := &Host{Config: Config{Broker: "mqtt://unused:1883", Node: "h"}}
	if err := h.Config.defaults(); err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if rec := h.Process(ctx, "u1", []byte("{nope")); rec.Error == "" || rec.Route.Topic != "wire/receipts/u1" {
		t.Fatalf("unparseable: %+v", rec)
	}
	raw, _ := json.Marshal(Envelope{ULID: "other", Atom: AtomCall, Call: &httpx.Request{URL: "http://x/"}})
	if rec := h.Process(ctx, "u2", raw); rec.Decision.Fired || !strings.Contains(rec.Decision.Reason, "does not match topic") {
		t.Fatalf("ulid mismatch: %+v", rec.Decision)
	}
	raw, _ = json.Marshal(Envelope{ULID: "u3", Atom: AtomCall, Call: &httpx.Request{URL: "http://x/"}, NotAfter: "2000-01-01T00:00:00Z"})
	if rec := h.Process(ctx, "u3", raw); rec.Decision.Fired || rec.Decision.Policy != "expiry" {
		t.Fatalf("expired: %+v", rec.Decision)
	}
	raw, _ = json.Marshal(Envelope{ULID: "u4", Atom: "queue"})
	if rec := h.Process(ctx, "u4", raw); rec.Decision.Fired || rec.Decision.Policy != "validate" {
		t.Fatalf("bad atom: %+v", rec.Decision)
	}
}

// TestMQTT_SchemasMatchTypes keeps envelope.schema.json / receipt.schema.json
// honest: every "required" key in the schema must appear in what the Go types
// emit, and every emitted key must be declared in the schema's properties.
func TestMQTT_SchemasMatchTypes(t *testing.T) {
	check := func(schemaFile string, v any) {
		t.Helper()
		raw, err := os.ReadFile(schemaFile)
		if err != nil {
			t.Fatal(err)
		}
		var schema struct {
			Required   []string                   `json:"required"`
			Properties map[string]json.RawMessage `json:"properties"`
		}
		if err := json.Unmarshal(raw, &schema); err != nil {
			t.Fatalf("%s: %v", schemaFile, err)
		}
		b, _ := json.Marshal(v)
		var got map[string]json.RawMessage
		_ = json.Unmarshal(b, &got)
		for _, k := range schema.Required {
			if _, ok := got[k]; !ok {
				t.Errorf("%s: required %q not emitted by the Go type", schemaFile, k)
			}
		}
		for k := range got {
			if _, ok := schema.Properties[k]; !ok {
				t.Errorf("%s: emitted %q not declared in schema properties", schemaFile, k)
			}
		}
	}
	check("envelope.schema.json", Envelope{Schema: EnvelopeSchema, ULID: "x", Seq: 1, Agent: "a", Atom: AtomCall,
		Call:     &httpx.Request{Method: "GET", URL: "http://x/", Headers: map[string]string{"A": "b"}, Body: "{}"},
		Channel:  &ChannelOp{Session: "fox", Method: "m", Params: json.RawMessage(`{}`), ID: 1},
		AuthSlot: "s", NotAfter: "2030-01-01T00:00:00Z", MaxMs: 1, Why: "w", ProposedAt: "2030-01-01T00:00:00Z"})
	check("receipt.schema.json", Receipt{Schema: ReceiptSchema, Contract: "v", ULID: "x", Seq: 1, Agent: "a", Atom: AtomCall, Why: "w",
		Decision: Decision{Fired: true, Policy: "p", Reason: "r", Egress: &Egress{Body: true, Headers: true}},
		Call:     &CallEcho{Method: "GET", URL: "http://x/"}, Channel: &ChannelEcho{Session: "fox", Method: "m"},
		Status: 200, LatencyMs: 1, BodyLen: 1, BodyDigest: "d", Body: "b", Truncated: true, Headers: map[string]string{"A": "b"},
		Witness: Witness{Line: "l", LedgerID: "1", Ledger: "2", DMs: "3"}, Error: "e", ProcessedAt: "t",
		Host: HostID{Node: "n", Actor: "a", Host: "h"}, Route: Route{Topic: "t", QoS: 1, Retain: true}})
}

func TestMQTT_ParseURL(t *testing.T) {
	for _, c := range []struct {
		in   string
		addr string
		tls  bool
		bad  bool
	}{
		{"mqtt://broker.example:1883", "broker.example:1883", false, false},
		{"mqtts://broker.example", "broker.example:8883", true, false},
		{"127.0.0.1:1884", "127.0.0.1:1884", false, false},
		{"tcp://h", "h:1883", false, false},
		{"http://h", "", false, true},
		{"", "", false, true},
	} {
		addr, useTLS, err := ParseURL(c.in)
		if c.bad {
			if err == nil {
				t.Errorf("ParseURL(%q) should fail", c.in)
			}
			continue
		}
		if err != nil || addr != c.addr || useTLS != c.tls {
			t.Errorf("ParseURL(%q) = %q %v %v", c.in, addr, useTLS, err)
		}
	}
}
