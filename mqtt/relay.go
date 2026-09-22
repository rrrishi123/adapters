package mqtt

// The relay: two roles over one broker. Host holds the witnessed wire and a
// persistent subscription to <prefix>/commands/+; FarEnd proposes envelopes
// and consumes receipts. Both only ever dial OUT.

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/rrrishi123/adapters/internal/guard"
	"github.com/rrrishi123/adapters/internal/httpx"
	"github.com/rrrishi123/adapters/trace"
)

// Config is one node's view of the relay. Every field is per-node
// configuration; nothing here defaults to a machine.
type Config struct {
	Broker string // mqtt://host:1883 or mqtts://host:8883 (required)
	Prefix string // topic prefix; default "wire"
	Node   string // this node's name = MQTT client id + presence topic level; default os hostname

	Username    string // broker auth (optional)
	Password    string // broker auth (optional)
	CAFile      string // PEM bundle for mqtts (optional; system roots otherwise)
	TLSInsecure bool   // skip certificate verification (lab brokers only)

	KeepAlive time.Duration // default 30s
	QoS       byte          // delivery guarantee for commands and receipts; default 1
	Reconnect time.Duration // host: back-off between sessions; default 3s

	Collector   string        // host: witness base URL, e.g. http://127.0.0.1:7070
	Actor       string        // host: X-8-Actor declared on every fire; default Node
	MaxBody     int           // host: receipt body cap in bytes; default 8192
	FireTimeout time.Duration // host: default per-fire deadline; default 60s
	Slots       string        // host: auth-slot file ({"<slot>": {"type":"bearer","key":"..."}}); optional

	Log *log.Logger
}

func (c *Config) defaults() error {
	if strings.TrimSpace(c.Broker) == "" {
		return errors.New("mqtt: Config.Broker is required (-broker / MQTT_BROKER)")
	}
	if c.Prefix == "" {
		c.Prefix = "wire"
	}
	c.Prefix = strings.Trim(c.Prefix, "/")
	if !ValidTopic(c.Prefix) {
		return fmt.Errorf("mqtt: prefix %q is not a valid topic", c.Prefix)
	}
	if c.Node == "" {
		h, _ := os.Hostname()
		if h == "" {
			h = "node"
		}
		c.Node = h
	}
	if !SafeID(c.Node) {
		return fmt.Errorf("mqtt: node %q must be [A-Za-z0-9._-]+ (it is a client id and a topic level)", c.Node)
	}
	if c.KeepAlive == 0 {
		c.KeepAlive = 30 * time.Second
	}
	if c.QoS == 0 {
		c.QoS = 1
	}
	if c.QoS > 1 {
		return ErrQoS2
	}
	if c.Reconnect <= 0 {
		c.Reconnect = 3 * time.Second
	}
	if c.Actor == "" {
		c.Actor = c.Node
	}
	if c.MaxBody <= 0 {
		c.MaxBody = 8192
	}
	if c.FireTimeout <= 0 {
		c.FireTimeout = 60 * time.Second
	}
	return nil
}

// Topic joins levels under the prefix.
func (c Config) Topic(levels ...string) string {
	return c.Prefix + "/" + strings.Join(levels, "/")
}

// CommandTopic is where the far end publishes envelope <ulid>.
func (c Config) CommandTopic(ulid string) string { return c.Topic("commands", ulid) }

// ReceiptTopic is where the host publishes the (retained) receipt for <ulid>.
func (c Config) ReceiptTopic(ulid string) string { return c.Topic("receipts", ulid) }

// CommandsFilter is the host's held subscription.
func (c Config) CommandsFilter() string { return c.Topic("commands", "+") }

// ReceiptsFilter is how the host learns which ULIDs are already answered.
func (c Config) ReceiptsFilter() string { return c.Topic("receipts", "+") }

// PresenceTopic is the retained "I am here" (and last-will "I am gone") per node.
func (c Config) PresenceTopic(node string) string { return c.Topic("presence", node) }

// Presence is the payload on <prefix>/presence/<node>: a held connection made
// visible to anyone subscribed to the prefix.
type Presence struct {
	Node     string `json:"node"`
	Role     string `json:"role"`  // host | far
	State    string `json:"state"` // online | offline
	Contract string `json:"contract"`
	TS       string `json:"ts"` // RFC3339 (absent on the will: the broker publishes it later)
}

func (c Config) presence(role, state string, now bool) Message {
	p := Presence{Node: c.Node, Role: role, State: state, Contract: trace.Version}
	if now {
		p.TS = time.Now().UTC().Format(time.RFC3339)
	}
	raw, _ := json.Marshal(p)
	return Message{Topic: c.PresenceTopic(c.Node), Payload: raw, QoS: c.QoS, Retain: true}
}

func (c Config) tlsConfig() (*tls.Config, error) {
	cfg := &tls.Config{InsecureSkipVerify: c.TLSInsecure} //nolint:gosec // operator opt-in for lab brokers
	if c.CAFile != "" {
		pem, err := os.ReadFile(c.CAFile)
		if err != nil {
			return nil, fmt.Errorf("mqtt: ca file: %w", err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(pem) {
			return nil, fmt.Errorf("mqtt: ca file %s holds no certificates", c.CAFile)
		}
		cfg.RootCAs = pool
	}
	return cfg, nil
}

// options builds the client Options for a role. The last will is the presence
// "offline" message so a vanished node is visible to the other side.
func (c Config) options(role string, clean bool) (Options, error) {
	tc, err := c.tlsConfig()
	if err != nil {
		return Options{}, err
	}
	will := c.presence(role, "offline", false)
	return Options{
		URL: c.Broker, ClientID: c.Node, Username: c.Username, Password: c.Password,
		CleanSession: clean, KeepAlive: c.KeepAlive, Will: &will, TLS: tc, Log: c.Log,
	}, nil
}

func ulidOf(topic string) string {
	i := strings.LastIndexByte(topic, '/')
	if i < 0 {
		return topic
	}
	return topic[i+1:]
}

// --- Host ---

// Host is the node that owns the witnessed wire. Run holds its broker
// session open (a persistent session, so commands queue at the broker while
// the host is down), fires every approved envelope through the witness, and
// publishes a retained receipt.
type Host struct {
	Config Config
	Firer  Firer  // nil = CollectorFirer from Config
	Policy Policy // nil = Closed (fail shut); loopbacks set AllowAll explicitly
	Log    *log.Logger
	Now    func() time.Time // nil = time.Now

	mu    sync.Mutex
	seen  map[string]bool // ULIDs with a receipt already (retained receipts + our own)
	stats Stats
	ready chan struct{} // closed once the first session is subscribed (tests)
	once  sync.Once
}

// Stats counts what the host has done since it started.
type Stats struct {
	Sessions int `json:"sessions"`
	Commands int `json:"commands"`
	Fired    int `json:"fired"`
	Refused  int `json:"refused"`
	Errors   int `json:"errors"`
	Skipped  int `json:"skipped"` // already answered (idempotent)
}

// Stats returns a snapshot.
func (h *Host) Stats() Stats {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.stats
}

// Ready is closed once the host has a live, subscribed session.
func (h *Host) Ready() <-chan struct{} {
	h.once.Do(func() { h.ready = make(chan struct{}) })
	return h.ready
}

func (h *Host) init() {
	h.once.Do(func() { h.ready = make(chan struct{}) })
	h.mu.Lock()
	if h.seen == nil {
		h.seen = map[string]bool{}
	}
	h.mu.Unlock()
}

func (h *Host) firer() Firer {
	if h.Firer != nil {
		return h.Firer
	}
	return &CollectorFirer{Collector: h.Config.Collector, Actor: h.Config.Actor}
}

func (h *Host) policy() Policy {
	if h.Policy != nil {
		return h.Policy
	}
	return Closed
}

func (h *Host) now() time.Time {
	if h.Now != nil {
		return h.Now()
	}
	return time.Now()
}

func (h *Host) logf(format string, a ...any) {
	if h.Log != nil {
		h.Log.Printf(format, a...)
	}
}

func (h *Host) markSeen(ulid string) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	was := h.seen[ulid]
	h.seen[ulid] = true
	return was
}

func (h *Host) count(f func(*Stats)) {
	h.mu.Lock()
	f(&h.stats)
	h.mu.Unlock()
}

// Run keeps a session alive until ctx ends, reconnecting after Config.Reconnect
// on loss. Because the session is persistent (CleanSession=false), commands
// published while the host was away are delivered on reconnect.
func (h *Host) Run(ctx context.Context) error {
	if err := h.Config.defaults(); err != nil {
		return err
	}
	for {
		err := h.Serve(ctx)
		if ctx.Err() != nil {
			return ctx.Err()
		}
		h.logf("mqtt host: session ended: %v — reconnecting in %s", err, h.Config.Reconnect)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(h.Config.Reconnect):
		}
	}
}

// Serve runs exactly one broker session and returns when it ends (nil when
// ctx was cancelled and the session closed cleanly).
func (h *Host) Serve(ctx context.Context) error {
	if err := h.Config.defaults(); err != nil {
		return err
	}
	h.init()
	cfg := h.Config
	opts, err := cfg.options("host", false)
	if err != nil {
		return err
	}
	c, err := Connect(ctx, opts)
	if err != nil {
		return err
	}
	defer c.Close()
	h.count(func(s *Stats) { s.Sessions++ })

	// receipts first: the retained ones tell us which ULIDs are already answered
	receipts, err := c.SubscribeMessages(ctx, cfg.ReceiptsFilter(), cfg.QoS)
	if err != nil {
		return err
	}
	commands, err := c.SubscribeMessages(ctx, cfg.CommandsFilter(), cfg.QoS)
	if err != nil {
		return err
	}
	if err := c.PublishMessage(ctx, cfg.presence("host", "online", true)); err != nil {
		return err
	}
	h.logf("mqtt host: node=%s broker=%s prefix=%s session_present=%v collector=%s actor=%s", cfg.Node, cfg.Broker, cfg.Prefix, c.SessionPresent(), cfg.Collector, cfg.Actor)
	h.mu.Lock()
	select {
	case <-h.ready:
	default:
		close(h.ready)
	}
	h.mu.Unlock()

	drainReceipts := func() {
		for {
			select {
			case m, ok := <-receipts:
				if !ok {
					return
				}
				h.markSeen(ulidOf(m.Topic))
			default:
				return
			}
		}
	}
	for {
		select {
		case <-ctx.Done():
			// graceful: say goodbye so the broker does not fire our will, keep the session
			pctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			_ = c.PublishMessage(pctx, cfg.presence("host", "offline", true))
			cancel()
			return nil
		case <-c.Done():
			return c.Err()
		case m, ok := <-receipts:
			if !ok {
				return c.Err()
			}
			h.markSeen(ulidOf(m.Topic))
		case m, ok := <-commands:
			if !ok {
				return c.Err()
			}
			drainReceipts() // anything retained that was enqueued before this command
			ulid := ulidOf(m.Topic)
			h.count(func(s *Stats) { s.Commands++ })
			if h.markSeen(ulid) {
				h.count(func(s *Stats) { s.Skipped++ })
				h.logf("mqtt host: %s already answered — skipped (idempotent)", ulid)
				continue
			}
			rec := h.Process(ctx, ulid, m.Payload)
			raw, _ := json.Marshal(rec)
			pctx, cancel := context.WithTimeout(ctx, 10*time.Second)
			err := c.PublishMessage(pctx, Message{Topic: rec.Route.Topic, Payload: raw, QoS: rec.Route.QoS, Retain: rec.Route.Retain})
			cancel()
			if err != nil {
				// the receipt never reached the broker: forget the ULID so a redelivery is answered
				h.mu.Lock()
				delete(h.seen, ulid)
				h.mu.Unlock()
				return fmt.Errorf("mqtt host: publish receipt %s: %w", ulid, err)
			}
			h.count(func(s *Stats) {
				switch {
				case rec.Decision.Fired && rec.Error == "":
					s.Fired++
				case rec.Decision.Fired:
					s.Errors++
				default:
					s.Refused++
				}
			})
			h.logf("mqtt host: %s %s → fired=%v status=%d %s", ulid, describe(rec), rec.Decision.Fired, rec.Status, firstNonEmpty(rec.Error, rec.Decision.Reason, rec.Witness.Line))
		}
	}
}

func describe(r *Receipt) string {
	switch {
	case r.Channel != nil:
		return r.Channel.Method + " @" + r.Channel.Session
	case r.Call != nil:
		return r.Call.Method + " " + r.Call.URL
	}
	return r.Atom
}

func firstNonEmpty(xs ...string) string {
	for _, x := range xs {
		if x != "" {
			return x
		}
	}
	return ""
}

// Process turns one envelope payload into a receipt. Every path —
// unparseable, invalid, expired, refused, failed, fired — yields a receipt,
// because the far end has no other way to learn what happened.
func (h *Host) Process(ctx context.Context, ulid string, raw []byte) *Receipt {
	h.init()
	cfg := h.Config
	hostname, _ := os.Hostname()
	rec := &Receipt{
		Schema:      ReceiptSchema,
		Contract:    trace.Version,
		ULID:        ulid,
		Atom:        AtomCall,
		ProcessedAt: h.now().UTC().Format(time.RFC3339),
		Host:        HostID{Node: cfg.Node, Actor: cfg.Actor, Host: hostname},
		Route:       Route{Topic: cfg.ReceiptTopic(ulid), QoS: cfg.QoS, Retain: true},
	}
	var env Envelope
	if err := json.Unmarshal(raw, &env); err != nil {
		rec.Error = "unparseable envelope: " + err.Error()
		return rec
	}
	rec.Seq, rec.Agent, rec.Why = env.Seq, env.Agent, env.Why
	if env.Atom != "" {
		rec.Atom = env.Atom
	}
	if err := env.Validate(); err != nil {
		rec.Decision = Decision{Fired: false, Policy: "validate", Reason: err.Error()}
		return rec
	}
	rec.Atom = env.Atom
	switch env.Atom {
	case AtomCall:
		rec.Call = &CallEcho{Method: env.Call.Method, URL: env.Call.URL}
	case AtomChannel:
		rec.Channel = &ChannelEcho{Session: env.Channel.Session, Method: env.Channel.Method}
	}
	if env.ULID != ulid {
		rec.Decision = Decision{Fired: false, Policy: "validate", Reason: fmt.Sprintf("envelope ulid %q does not match topic %q", env.ULID, ulid)}
		return rec
	}
	if env.Expired(h.now()) {
		rec.Decision = Decision{Fired: false, Policy: "expiry", Reason: "expired (not_after " + env.NotAfter + ")"}
		return rec
	}
	// the host decides — the far end only proposed
	d := h.policy().Decide(&env)
	rec.Decision = d
	if !d.Fired {
		return rec
	}
	timeout := cfg.FireTimeout
	if env.MaxMs > 0 {
		timeout = time.Duration(env.MaxMs) * time.Millisecond
	}
	fctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	var out *Outcome
	var err error
	authenticated := false
	switch env.Atom {
	case AtomCall:
		call := *env.Call
		if env.AuthSlot != "" {
			auth, err := h.resolveSlot(env.AuthSlot)
			if err != nil {
				rec.Decision = Decision{Fired: false, Policy: "auth-slot", Reason: err.Error()}
				return rec
			}
			auth.Apply(&call) // the credential exists only here, on this side, in memory
			authenticated, rec.Authenticated = true, true
		}
		out, err = h.firer().FireCall(fctx, call)
	case AtomChannel:
		out, err = h.firer().FireChannel(fctx, *env.Channel)
	}
	if err != nil {
		rec.Error = "fire failed: " + err.Error()
		return rec
	}
	// G3: the receipt travels a broker the host does not own — egress-gated,
	// headers allowlisted, slot-authenticated body digest-only unless opted in
	egress := d.Egress
	if egress == nil {
		egress = guard.DefaultEgress()
	}
	rec.Status = out.Status
	rec.LatencyMs = out.Latency.Milliseconds()
	rec.BodyLen = len(out.Body)
	rec.BodyDigest = Digest(out.Body)
	if egress.PublishBody(authenticated) {
		if max := cfg.MaxBody; len(out.Body) > max {
			rec.Body, rec.Truncated = string(out.Body[:max]), true
		} else {
			rec.Body = string(out.Body)
		}
	}
	if egress.PublishHeaders() {
		rec.Headers = guard.ResponseHeaders(out.Headers)
	}
	rec.Witness = Witness{
		Line:     out.Headers.Get("X-8-Witness"),
		LedgerID: out.Headers.Get("X-8-Ledger-Id"),
		Ledger:   out.Headers.Get("X-8-Ledger"),
		DMs:      out.Headers.Get("X-8-DMs"),
	}
	return rec
}

// resolveSlot maps an auth_slot NAME to a credential from the host-side slots
// file ({"<slot>": {"type":"bearer","key":"..."}, ...}) — the one thing that
// never travels a topic.
func (h *Host) resolveSlot(slot string) (httpx.Auth, error) {
	if h.Config.Slots == "" {
		return httpx.Auth{}, fmt.Errorf("auth_slot %q: no slots file configured on the host (-slots / MQTT_SLOTS)", slot)
	}
	b, err := os.ReadFile(h.Config.Slots)
	if err != nil {
		return httpx.Auth{}, fmt.Errorf("auth_slot %q: slots file unavailable: %v", slot, err)
	}
	var slots map[string]httpx.Auth
	if err := json.Unmarshal(b, &slots); err != nil {
		return httpx.Auth{}, fmt.Errorf("auth_slot %q: slots file malformed: %v", slot, err)
	}
	a, ok := slots[slot]
	if !ok {
		return httpx.Auth{}, fmt.Errorf("auth_slot %q not resolvable host-side", slot)
	}
	return a, nil
}

// --- FarEnd ---

// FarEnd is the proposing node (the sandbox). It only ever dials out to the
// broker; the receipt comes back on its own held subscription — or, if it
// was away, as the retained message the next time it asks.
type FarEnd struct {
	Config Config
	Log    *log.Logger
}

func (f *FarEnd) logf(format string, a ...any) {
	if f.Log != nil {
		f.Log.Printf(format, a...)
	}
}

// Connect opens the far end's session (clean; its state is the receipts,
// which the broker retains) and announces presence.
func (f *FarEnd) Connect(ctx context.Context) (*Client, error) {
	if err := f.Config.defaults(); err != nil {
		return nil, err
	}
	opts, err := f.Config.options("far", true)
	if err != nil {
		return nil, err
	}
	c, err := Connect(ctx, opts)
	if err != nil {
		return nil, err
	}
	if err := c.PublishMessage(ctx, f.Config.presence("far", "online", true)); err != nil {
		c.Close()
		return nil, err
	}
	return c, nil
}

// Propose publishes one envelope to <prefix>/commands/<ulid>. It validates
// locally first (fail fast, and never put a credential on the broker).
func (f *FarEnd) Propose(ctx context.Context, c *Client, env *Envelope) error {
	if err := f.Config.defaults(); err != nil {
		return err
	}
	if env.Schema == "" {
		env.Schema = EnvelopeSchema
	}
	if env.Agent == "" {
		env.Agent = f.Config.Node
	}
	if env.ProposedAt == "" {
		env.ProposedAt = time.Now().UTC().Format(time.RFC3339)
	}
	if err := env.Validate(); err != nil {
		return err
	}
	raw, err := json.Marshal(env)
	if err != nil {
		return err
	}
	f.logf("mqtt far: propose %s → %s", env.ULID, f.Config.CommandTopic(env.ULID))
	return c.PublishMessage(ctx, Message{Topic: f.Config.CommandTopic(env.ULID), Payload: raw, QoS: f.Config.QoS})
}

// Await subscribes to the receipt for ulid and returns the first one — the
// live delivery, or the retained copy if the host answered while we were away.
func (f *FarEnd) Await(ctx context.Context, c *Client, ulid string) (*Receipt, error) {
	ch, err := c.SubscribeMessages(ctx, f.Config.ReceiptTopic(ulid), f.Config.QoS)
	if err != nil {
		return nil, err
	}
	defer func() {
		uctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		_ = c.Unsubscribe(uctx, f.Config.ReceiptTopic(ulid))
		cancel()
	}()
	select {
	case m, ok := <-ch:
		if !ok {
			return nil, fmt.Errorf("mqtt far: session ended before the receipt for %s: %v", ulid, c.Err())
		}
		var rec Receipt
		if err := json.Unmarshal(m.Payload, &rec); err != nil {
			return nil, fmt.Errorf("mqtt far: receipt for %s unparseable: %w", ulid, err)
		}
		return &rec, nil
	case <-ctx.Done():
		return nil, fmt.Errorf("mqtt far: waiting for the receipt for %s: %w", ulid, ctx.Err())
	}
}

// Fire is the one-shot: connect, hold the receipt subscription open FIRST (so
// nothing can slip between publish and subscribe), propose, await, close.
func (f *FarEnd) Fire(ctx context.Context, env *Envelope) (*Receipt, error) {
	c, err := f.Connect(ctx)
	if err != nil {
		return nil, err
	}
	defer c.Close()
	if env.Schema == "" {
		env.Schema = EnvelopeSchema
	}
	if err := env.Validate(); err != nil {
		return nil, err
	}
	ch, err := c.SubscribeMessages(ctx, f.Config.ReceiptTopic(env.ULID), f.Config.QoS)
	if err != nil {
		return nil, err
	}
	// a retained receipt means this ULID was already answered: return it, do not re-propose
	select {
	case m := <-ch:
		var rec Receipt
		if err := json.Unmarshal(m.Payload, &rec); err == nil {
			f.logf("mqtt far: %s already answered (retained receipt)", env.ULID)
			return &rec, nil
		}
	case <-time.After(150 * time.Millisecond):
	}
	if err := f.Propose(ctx, c, env); err != nil {
		return nil, err
	}
	select {
	case m, ok := <-ch:
		if !ok {
			return nil, fmt.Errorf("mqtt far: session ended before the receipt for %s: %v", env.ULID, c.Err())
		}
		var rec Receipt
		if err := json.Unmarshal(m.Payload, &rec); err != nil {
			return nil, fmt.Errorf("mqtt far: receipt for %s unparseable: %w", env.ULID, err)
		}
		return &rec, nil
	case <-ctx.Done():
		return nil, fmt.Errorf("mqtt far: waiting for the receipt for %s: %w", env.ULID, ctx.Err())
	}
}
