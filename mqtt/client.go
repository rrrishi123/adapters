package mqtt

// Client — a real MQTT 3.1.1 client, pure stdlib. One Client is one held
// connection to a broker: the CHANNEL atom. You produce into it (PublishMessage)
// and consume from it (SubscribeMessages); everything else — QoS 1 acks,
// retain, wildcard filters, keep-alive pings, last will, username/password,
// TLS — is the app-routing dialect the adapter carries so the wire never has to.

import (
	"bufio"
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"log"
	"net"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// Options configures one connection. Nothing here has a machine-specific
// default: the broker URL always comes from the caller (flags / MQTT_* env).
type Options struct {
	URL          string        // mqtt://host:1883, mqtts://host:8883 (also tcp://, ssl://, tls://, or bare host:port)
	ClientID     string        // required by most brokers; the relay uses the node name
	Username     string        // broker auth (optional)
	Password     string        // broker auth (optional)
	CleanSession bool          // false = the broker keeps subscriptions + QoS1 backlog while we are away
	KeepAlive    time.Duration // 0 = 30s; the client pings at half this and fails the session at 1.5×
	Will         *Message      // last-will: published by the broker if we vanish without DISCONNECT
	TLS          *tls.Config   // nil = defaults when the scheme is TLS
	DialTimeout  time.Duration // 0 = 5s
	MaxPacket    int           // inbound packet cap; 0 = 4 MiB
	Log          *log.Logger   // nil = quiet
}

// ErrQoS2 — exactly-once is not offered. A relay gets its idempotency from the
// receipt (a ULID answered once is never fired again), not from QoS 2's
// four-step handshake, so the adapter deliberately stops at QoS 1.
var ErrQoS2 = errors.New("mqtt: QoS 2 is not offered by this adapter (use QoS 1 + idempotent receipts)")

// Client is one live session with a broker.
type Client struct {
	opts Options
	conn net.Conn
	r    *bufio.Reader

	wmu sync.Mutex // serializes writes on conn

	mu       sync.Mutex
	pid      uint16
	acks     map[uint16]chan []byte // packet id → waiter for PUBACK/SUBACK/UNSUBACK
	subs     []*subscription
	unrouted []Message // deliveries that matched no subscription yet (a resumed session pushes its backlog at CONNACK, before we re-subscribe)

	lastRx atomic.Int64 // unix nanos of the last byte read (keep-alive liveness)
	done   chan struct{}
	err    error
	once   sync.Once

	sessionPresent bool
}

type subscription struct {
	filter string
	ch     chan Message
}

// ParseURL resolves the broker URL into a dial address and whether to use TLS.
func ParseURL(raw string) (addr string, useTLS bool, err error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return "", false, errors.New("mqtt: broker URL is empty (set -broker / MQTT_BROKER)")
	}
	if !strings.Contains(raw, "://") {
		raw = "mqtt://" + raw
	}
	u, err := url.Parse(raw)
	if err != nil {
		return "", false, fmt.Errorf("mqtt: broker URL: %w", err)
	}
	switch strings.ToLower(u.Scheme) {
	case "mqtt", "tcp":
		useTLS = false
	case "mqtts", "ssl", "tls":
		useTLS = true
	default:
		return "", false, fmt.Errorf("mqtt: unsupported scheme %q (mqtt:// or mqtts://)", u.Scheme)
	}
	host, port := u.Hostname(), u.Port()
	if host == "" {
		return "", false, errors.New("mqtt: broker URL has no host")
	}
	if port == "" {
		if useTLS {
			port = "8883"
		} else {
			port = "1883"
		}
	}
	return net.JoinHostPort(host, port), useTLS, nil
}

// Connect dials the broker, performs CONNECT/CONNACK, and starts the read and
// keep-alive loops. The returned Client is the held CHANNEL.
func Connect(ctx context.Context, opts Options) (*Client, error) {
	addr, useTLS, err := ParseURL(opts.URL)
	if err != nil {
		return nil, err
	}
	if opts.KeepAlive == 0 {
		opts.KeepAlive = 30 * time.Second
	}
	if opts.DialTimeout == 0 {
		opts.DialTimeout = 5 * time.Second
	}
	if opts.MaxPacket == 0 {
		opts.MaxPacket = 4 << 20
	}
	if opts.Will != nil && !ValidTopic(opts.Will.Topic) {
		return nil, fmt.Errorf("mqtt: will topic %q is not publishable", opts.Will.Topic)
	}
	d := &net.Dialer{Timeout: opts.DialTimeout}
	var conn net.Conn
	if useTLS {
		cfg := opts.TLS
		if cfg == nil {
			cfg = &tls.Config{}
		}
		if cfg.ServerName == "" {
			h, _, _ := net.SplitHostPort(addr)
			c2 := cfg.Clone()
			c2.ServerName = h
			cfg = c2
		}
		conn, err = tls.DialWithDialer(d, "tcp", addr, cfg)
	} else {
		conn, err = d.DialContext(ctx, "tcp", addr)
	}
	if err != nil {
		return nil, fmt.Errorf("mqtt: dial %s: %w", addr, err)
	}
	c := &Client{
		opts: opts,
		conn: conn,
		r:    bufio.NewReader(conn),
		acks: map[uint16]chan []byte{},
		done: make(chan struct{}),
	}
	c.lastRx.Store(time.Now().UnixNano())

	// CONNECT (3.1)
	body := putString(nil, "MQTT")
	body = append(body, 0x04) // protocol level 4 = 3.1.1
	var flags byte
	if opts.CleanSession {
		flags |= 0x02
	}
	if opts.Will != nil {
		flags |= 0x04 | (opts.Will.QoS&0x3)<<3
		if opts.Will.Retain {
			flags |= 0x20
		}
	}
	if opts.Password != "" {
		flags |= 0x40
	}
	if opts.Username != "" {
		flags |= 0x80
	}
	body = append(body, flags)
	body = putUint16(body, uint16(opts.KeepAlive/time.Second))
	body = putString(body, opts.ClientID)
	if opts.Will != nil {
		body = putString(body, opts.Will.Topic)
		body = putBytes(body, opts.Will.Payload)
	}
	if opts.Username != "" {
		body = putString(body, opts.Username)
	}
	if opts.Password != "" {
		body = putBytes(body, []byte(opts.Password))
	}
	dl := time.Now().Add(opts.DialTimeout)
	if t, ok := ctx.Deadline(); ok && t.Before(dl) {
		dl = t
	}
	_ = conn.SetDeadline(dl)
	if _, err := conn.Write(packet(typeCONNECT, body)); err != nil {
		conn.Close()
		return nil, fmt.Errorf("mqtt: CONNECT: %w", err)
	}
	header, ack, err := readPacket(c.r, opts.MaxPacket)
	if err != nil || header&0xf0 != typeCONNACK || len(ack) < 2 {
		conn.Close()
		return nil, fmt.Errorf("mqtt: no CONNACK (err=%v header=%#x)", err, header)
	}
	if ack[1] != connAccepted {
		conn.Close()
		return nil, fmt.Errorf("mqtt: connection refused by broker: %s", connackReason(ack[1]))
	}
	c.sessionPresent = ack[0]&0x01 != 0
	_ = conn.SetDeadline(time.Time{})
	go c.readLoop()
	go c.pinger()
	return c, nil
}

func connackReason(rc byte) string {
	switch rc {
	case connBadProtocol:
		return "unacceptable protocol version (rc=1)"
	case connIDRejected:
		return "client id rejected (rc=2)"
	case 0x03:
		return "server unavailable (rc=3)"
	case connBadAuth:
		return "bad username or password (rc=4)"
	case connNotAuthorize:
		return "not authorized (rc=5)"
	}
	return fmt.Sprintf("rc=%d", rc)
}

// SessionPresent reports whether the broker resumed a persistent session
// (CleanSession=false and the broker still held state for this ClientID).
func (c *Client) SessionPresent() bool { return c.sessionPresent }

// Done is closed when the session ends for any reason; Err says why.
func (c *Client) Done() <-chan struct{} { return c.done }

// Err returns the terminal error (nil while alive, or after a clean Close).
func (c *Client) Err() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.err
}

func (c *Client) logf(format string, a ...any) {
	if c.opts.Log != nil {
		c.opts.Log.Printf(format, a...)
	}
}

func (c *Client) write(p []byte) error {
	c.wmu.Lock()
	defer c.wmu.Unlock()
	_ = c.conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
	_, err := c.conn.Write(p)
	return err
}

// fail ends the session with err (idempotent) and wakes every waiter.
func (c *Client) fail(err error) {
	c.once.Do(func() {
		c.mu.Lock()
		c.err = err
		acks := c.acks
		c.acks = map[uint16]chan []byte{}
		subs := c.subs
		c.subs = nil
		c.mu.Unlock()
		_ = c.conn.Close()
		for _, ch := range acks {
			close(ch)
		}
		for _, s := range subs {
			close(s.ch)
		}
		close(c.done)
	})
}

// Close sends DISCONNECT (so the broker does NOT publish our will) and tears
// the session down. With CleanSession=false the broker keeps our
// subscriptions and QoS1 backlog for the next Connect with the same ClientID.
func (c *Client) Close() error {
	_ = c.write(packet(typeDISCONNECT, nil))
	c.fail(nil)
	return nil
}

func (c *Client) readLoop() {
	for {
		header, body, err := readPacket(c.r, c.opts.MaxPacket)
		if err != nil {
			c.fail(fmt.Errorf("mqtt: session ended: %w", err))
			return
		}
		c.lastRx.Store(time.Now().UnixNano())
		switch header & 0xf0 {
		case typePUBLISH:
			m, pid, err := decodePublish(header, body)
			if err != nil {
				c.fail(err)
				return
			}
			if m.QoS == 1 {
				if err := c.write(packet(typePUBACK, putUint16(nil, pid))); err != nil {
					c.fail(err)
					return
				}
			}
			c.dispatch(m)
		case typePUBACK, typeSUBACK, typeUNSUBACK:
			pid, rest, ok := getUint16(body)
			if !ok {
				continue
			}
			c.mu.Lock()
			ch := c.acks[pid]
			delete(c.acks, pid)
			c.mu.Unlock()
			if ch != nil {
				ch <- append([]byte{}, rest...)
				close(ch)
			}
		case typePINGRESP:
			// lastRx already advanced — that is the liveness signal
		default:
			c.logf("mqtt: ignoring packet type %#x", header&0xf0)
		}
	}
}

// maxUnrouted bounds the backlog held for subscriptions not yet re-opened.
const maxUnrouted = 1024

// dispatch hands a delivered message to every subscription whose filter
// matches. Delivery blocks (bounded by the session ending): at QoS 1 the
// broker has our PUBACK, so a slow consumer must apply backpressure to the
// socket rather than lose the message. A message no subscription claims is
// held: with CleanSession=false the broker flushes the backlog right after
// CONNACK, before the caller has re-subscribed, and SubscribeMessages replays
// it then.
func (c *Client) dispatch(m Message) {
	c.mu.Lock()
	var targets []*subscription
	for _, s := range c.subs {
		if MatchTopic(s.filter, m.Topic) {
			targets = append(targets, s)
		}
	}
	if len(targets) == 0 {
		if len(c.unrouted) >= maxUnrouted {
			c.unrouted = c.unrouted[1:]
		}
		c.unrouted = append(c.unrouted, m)
	}
	c.mu.Unlock()
	for _, s := range targets {
		select {
		case s.ch <- m:
		case <-c.done:
			return
		}
	}
}

func (c *Client) pinger() {
	ka := c.opts.KeepAlive
	t := time.NewTicker(ka / 2)
	defer t.Stop()
	for {
		select {
		case <-c.done:
			return
		case <-t.C:
			if time.Since(time.Unix(0, c.lastRx.Load())) > ka+ka/2 {
				c.fail(fmt.Errorf("mqtt: keep-alive timeout (no bytes from broker in %s)", ka+ka/2))
				return
			}
			if err := c.write(packet(typePINGREQ, nil)); err != nil {
				c.fail(fmt.Errorf("mqtt: PINGREQ: %w", err))
				return
			}
		}
	}
}

func (c *Client) nextID() (uint16, chan []byte) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for {
		c.pid++
		if c.pid == 0 {
			c.pid = 1
		}
		if _, taken := c.acks[c.pid]; !taken {
			break
		}
	}
	ch := make(chan []byte, 1)
	c.acks[c.pid] = ch
	return c.pid, ch
}

func (c *Client) await(ctx context.Context, pid uint16, ch chan []byte, what string) ([]byte, error) {
	select {
	case b, ok := <-ch:
		if !ok {
			return nil, fmt.Errorf("mqtt: session ended before %s: %v", what, c.Err())
		}
		return b, nil
	case <-ctx.Done():
		c.mu.Lock()
		delete(c.acks, pid)
		c.mu.Unlock()
		return nil, fmt.Errorf("mqtt: %s: %w", what, ctx.Err())
	case <-c.done:
		return nil, fmt.Errorf("mqtt: session ended before %s: %v", what, c.Err())
	}
}

// PublishMessage produces one message into the channel. At QoS 1 it returns
// only once the broker has PUBACKed (the broker now owns delivery); at QoS 0
// it returns once the bytes are written.
func (c *Client) PublishMessage(ctx context.Context, m Message) error {
	if !ValidTopic(m.Topic) {
		return fmt.Errorf("mqtt: topic %q is not publishable (empty or has wildcards)", m.Topic)
	}
	if m.QoS >= 2 {
		return ErrQoS2
	}
	if m.QoS == 0 {
		return c.write(encodePublish(m, 0))
	}
	pid, ch := c.nextID()
	if err := c.write(encodePublish(m, pid)); err != nil {
		return err
	}
	_, err := c.await(ctx, pid, ch, "PUBACK for "+m.Topic)
	return err
}

// SubscribeMessages opens a consumer for a topic filter at the requested QoS
// (0 or 1) and returns the channel deliveries arrive on, once the broker's
// SUBACK confirms it. Retained messages for the filter arrive first, flagged
// Retain=true. The channel is closed when the session ends or on Unsubscribe.
func (c *Client) SubscribeMessages(ctx context.Context, filter string, qos byte) (<-chan Message, error) {
	if !ValidFilter(filter) {
		return nil, fmt.Errorf("mqtt: filter %q is malformed", filter)
	}
	if qos >= 2 {
		return nil, ErrQoS2
	}
	s := &subscription{filter: filter, ch: make(chan Message, 256)}
	c.mu.Lock()
	// replay what a resumed session already pushed for this filter, in order,
	// before the subscription goes live — then register it BEFORE the SUBACK so
	// nothing between SUBACK and retained delivery is lost either
	keep := c.unrouted[:0]
	for _, m := range c.unrouted {
		if !MatchTopic(filter, m.Topic) {
			keep = append(keep, m)
			continue
		}
		select {
		case s.ch <- m:
		default:
			keep = append(keep, m) // channel full: stays held for a later subscribe
		}
	}
	c.unrouted = keep
	c.subs = append(c.subs, s)
	c.mu.Unlock()
	pid, ch := c.nextID()
	body := putUint16(nil, pid)
	body = putString(body, filter)
	body = append(body, qos)
	if err := c.write(packet(pktSUBSCRIBE, body)); err != nil {
		c.removeSub(s)
		return nil, err
	}
	codes, err := c.await(ctx, pid, ch, "SUBACK for "+filter)
	if err != nil {
		c.removeSub(s)
		return nil, err
	}
	if len(codes) == 0 || codes[0] == 0x80 {
		c.removeSub(s)
		return nil, fmt.Errorf("mqtt: broker refused subscription to %q", filter)
	}
	return s.ch, nil
}

func (c *Client) removeSub(s *subscription) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for i, x := range c.subs {
		if x == s {
			c.subs = append(c.subs[:i], c.subs[i+1:]...)
			return
		}
	}
}

// Unsubscribe drops every consumer for filter and tells the broker.
func (c *Client) Unsubscribe(ctx context.Context, filter string) error {
	c.mu.Lock()
	var kept []*subscription
	var dropped []*subscription
	for _, s := range c.subs {
		if s.filter == filter {
			dropped = append(dropped, s)
		} else {
			kept = append(kept, s)
		}
	}
	c.subs = kept
	c.mu.Unlock()
	for _, s := range dropped {
		close(s.ch)
	}
	pid, ch := c.nextID()
	body := putString(putUint16(nil, pid), filter)
	if err := c.write(packet(pktUNSUBSCRIBE, body)); err != nil {
		return err
	}
	_, err := c.await(ctx, pid, ch, "UNSUBACK for "+filter)
	return err
}

// --- the July harness API, kept: loopback and the conformance tests use it ---

// Dial connects with a clean session and 3.1.1 defaults (QoS 0 API below).
func Dial(addr, clientID string) (*Client, error) {
	return Connect(context.Background(), Options{URL: addr, ClientID: clientID, CleanSession: true, DialTimeout: 3 * time.Second})
}

// Subscribe opens a QoS 0 CHANNEL for one topic and yields raw payloads.
func (c *Client) Subscribe(topic string) (<-chan []byte, error) {
	ms, err := c.SubscribeMessages(context.Background(), topic, 0)
	if err != nil {
		return nil, err
	}
	out := make(chan []byte, 8)
	go func() {
		defer close(out)
		for m := range ms {
			select {
			case out <- m.Payload:
			default: // QoS 0 semantics: a slow consumer drops
			}
		}
	}()
	return out, nil
}

// Publish fires one QoS 0 message into a topic — the July CALL-shaped probe.
func (c *Client) Publish(topic string, payload []byte) error {
	return c.PublishMessage(context.Background(), Message{Topic: topic, Payload: payload})
}
