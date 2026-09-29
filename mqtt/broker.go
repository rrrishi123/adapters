package mqtt

// Broker — the in-process MQTT 3.1.1 broker: the other end we own, so the
// adapter's shape (and its loopback) is proven without any external daemon.
// It speaks the same dialect the relay needs from a real broker (mosquitto,
// EMQX, HiveMQ, a cloud broker): QoS 0/1 with PUBACK, retained messages,
// '+'/'#' filters, keep-alive, last will, username/password, and PERSISTENT
// SESSIONS (CleanSession=false keeps subscriptions and queues QoS 1 messages
// while the client is away — the "store" in store-and-forward).
//
// Deliberately not a production broker: no QoS 2, no TLS listener, no
// persistence across process restarts, no ACLs beyond Auth.

import (
	"bufio"
	"fmt"
	"log"
	"net"
	"sync"
	"time"
)

// Broker routes PUBLISH to matching subscribers, holds retained messages, and
// keeps persistent sessions for absent clients.
type Broker struct {
	ln net.Listener

	// Auth decides CONNECT for a username/password pair; nil accepts everything.
	Auth func(username, password string) bool
	// Log receives one line per connect/disconnect when set.
	Log *log.Logger
	// MaxPacket caps an inbound packet body (0 = 1 MiB).
	MaxPacket int

	mu       sync.Mutex
	sessions map[string]*session
	retained map[string]Message
}

type session struct {
	id    string
	clean bool
	subs  map[string]byte // filter → granted QoS

	wmu  sync.Mutex
	conn net.Conn // nil while the client is away
	gen  int      // connection generation, so a stale handler cannot detach a newer one

	queue []Message // QoS 1 backlog held while away (persistent session)
	will  *Message
	pid   uint16
}

// NewBroker binds a loopback broker on an ephemeral port and starts serving.
func NewBroker() (*Broker, error) {
	return Listen("127.0.0.1:0")
}

// Listen binds a broker on addr (host:port) and starts serving.
func Listen(addr string) (*Broker, error) {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, err
	}
	b := &Broker{ln: ln, sessions: map[string]*session{}, retained: map[string]Message{}}
	go b.serve()
	return b, nil
}

// Addr is the broker's dial address (host:port), usable as an Options.URL.
func (b *Broker) Addr() string { return b.ln.Addr().String() }

// URL is the broker's mqtt:// URL.
func (b *Broker) URL() string { return "mqtt://" + b.Addr() }

// Close stops accepting connections. Live sessions end as their sockets close.
func (b *Broker) Close() error { return b.ln.Close() }

// Retained returns a copy of the retained message for topic, if any.
func (b *Broker) Retained(topic string) (Message, bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	m, ok := b.retained[topic]
	return m, ok
}

func (b *Broker) logf(format string, a ...any) {
	if b.Log != nil {
		b.Log.Printf(format, a...)
	}
}

func (b *Broker) serve() {
	for {
		conn, err := b.ln.Accept()
		if err != nil {
			return
		}
		go b.handle(conn)
	}
}

func (b *Broker) maxPacket() int {
	if b.MaxPacket > 0 {
		return b.MaxPacket
	}
	return 1 << 20
}

// handle runs one client connection from CONNECT to close.
func (b *Broker) handle(conn net.Conn) {
	defer conn.Close()
	r := bufio.NewReader(conn)
	_ = conn.SetReadDeadline(time.Now().Add(10 * time.Second)) // CONNECT must come promptly
	header, body, err := readPacket(r, b.maxPacket())
	if err != nil || header&0xf0 != typeCONNECT {
		return
	}
	s, keepAlive, rc, present := b.connect(body, conn)
	if rc != connAccepted {
		_, _ = conn.Write(packet(typeCONNACK, []byte{0x00, rc}))
		return
	}
	ack := byte(0)
	if present {
		ack = 1
	}
	if _, err := conn.Write(packet(typeCONNACK, []byte{ack, connAccepted})); err != nil {
		return
	}
	gen := s.gen
	b.logf("mqtt broker: %s connected (clean=%v present=%v keepalive=%s)", s.id, s.clean, present, keepAlive)
	b.flush(s) // persistent session: deliver what queued while away

	graceful := false
	for {
		if keepAlive > 0 {
			_ = conn.SetReadDeadline(time.Now().Add(keepAlive + keepAlive/2))
		} else {
			_ = conn.SetReadDeadline(time.Time{})
		}
		header, body, err := readPacket(r, b.maxPacket())
		if err != nil {
			break
		}
		switch header & 0xf0 {
		case typePUBLISH:
			m, pid, err := decodePublish(header, body)
			if err != nil || !ValidTopic(m.Topic) {
				goto out
			}
			if m.QoS == 2 {
				goto out // not offered; a spec-compliant peer never sends it unless we granted it
			}
			if m.QoS == 1 {
				s.write(packet(typePUBACK, putUint16(nil, pid)))
			}
			b.publish(m)
		case typePUBACK:
			// subscriber acknowledged a QoS 1 delivery — nothing to retransmit in-process
		case typeSUBSCRIBE:
			pid, rest, ok := getUint16(body)
			if !ok {
				goto out
			}
			var codes []byte
			var filters []string
			for len(rest) > 0 {
				f, r2, ok := getString(rest)
				if !ok || len(r2) < 1 {
					goto out
				}
				q := r2[0] & 0x3
				rest = r2[1:]
				if !ValidFilter(f) {
					codes = append(codes, 0x80)
					continue
				}
				if q > 1 {
					q = 1 // QoS 2 downgraded to 1 (granted QoS is what SUBACK says)
				}
				b.mu.Lock()
				s.subs[f] = q
				b.mu.Unlock()
				codes = append(codes, q)
				filters = append(filters, f)
			}
			s.write(packet(typeSUBACK, append(putUint16(nil, pid), codes...)))
			b.sendRetained(s, filters)
		case typeUNSUBSCRIBE:
			pid, rest, ok := getUint16(body)
			if !ok {
				goto out
			}
			for len(rest) > 0 {
				f, r2, ok := getString(rest)
				if !ok {
					goto out
				}
				rest = r2
				b.mu.Lock()
				delete(s.subs, f)
				b.mu.Unlock()
			}
			s.write(packet(typeUNSUBACK, putUint16(nil, pid)))
		case typePINGREQ:
			s.write(packet(typePINGRESP, nil))
		case typeDISCONNECT:
			graceful = true
			goto out
		default:
			goto out
		}
	}
out:
	b.detach(s, gen, graceful)
}

// connect parses CONNECT and binds the connection to a (possibly resumed) session.
func (b *Broker) connect(body []byte, conn net.Conn) (s *session, keepAlive time.Duration, rc byte, present bool) {
	name, rest, ok := getString(body)
	if !ok || name != "MQTT" || len(rest) < 4 {
		return nil, 0, connBadProtocol, false
	}
	if rest[0] != 0x04 {
		return nil, 0, connBadProtocol, false
	}
	flags := rest[1]
	ka, rest, _ := getUint16(rest[2:])
	keepAlive = time.Duration(ka) * time.Second
	clientID, rest, ok := getString(rest)
	if !ok {
		return nil, 0, connBadProtocol, false
	}
	clean := flags&0x02 != 0
	var will *Message
	if flags&0x04 != 0 {
		t, r2, ok := getString(rest)
		if !ok {
			return nil, 0, connBadProtocol, false
		}
		p, r3, ok := getString(r2)
		if !ok {
			return nil, 0, connBadProtocol, false
		}
		rest = r3
		will = &Message{Topic: t, Payload: []byte(p), QoS: (flags >> 3) & 0x3, Retain: flags&0x20 != 0}
	}
	var user, pass string
	if flags&0x80 != 0 {
		if user, rest, ok = getString(rest); !ok {
			return nil, 0, connBadProtocol, false
		}
	}
	if flags&0x40 != 0 {
		if pass, _, ok = getString(rest); !ok {
			return nil, 0, connBadProtocol, false
		}
	}
	if clientID == "" {
		if !clean {
			return nil, 0, connIDRejected, false // 3.1.3-8: empty id requires clean session
		}
		clientID = fmt.Sprintf("anon-%d", time.Now().UnixNano())
	}
	if b.Auth != nil && !b.Auth(user, pass) {
		return nil, 0, connBadAuth, false
	}

	b.mu.Lock()
	defer b.mu.Unlock()
	old := b.sessions[clientID]
	if old != nil && old.conn != nil {
		old.wmu.Lock()
		c := old.conn
		old.conn = nil
		old.wmu.Unlock()
		_ = c.Close() // takeover: the newer connection wins (3.1.4-2)
	}
	if clean || old == nil {
		s = &session{id: clientID, clean: clean, subs: map[string]byte{}}
		b.sessions[clientID] = s
	} else {
		s = old
		present = true
	}
	s.gen++
	s.will = will
	s.wmu.Lock()
	s.conn = conn
	s.wmu.Unlock()
	return s, keepAlive, connAccepted, present
}

// detach ends a connection's binding to its session: publishes the will on an
// abnormal end, drops a clean session, keeps a persistent one for later.
func (b *Broker) detach(s *session, gen int, graceful bool) {
	b.mu.Lock()
	if s.gen != gen {
		b.mu.Unlock()
		return // a newer connection took this session over; it owns the lifecycle now
	}
	s.wmu.Lock()
	s.conn = nil
	s.wmu.Unlock()
	will := s.will
	s.will = nil
	if s.clean {
		delete(b.sessions, s.id)
	}
	b.mu.Unlock()
	b.logf("mqtt broker: %s disconnected (graceful=%v)", s.id, graceful)
	if !graceful && will != nil {
		b.publish(*will)
	}
}

func (s *session) write(p []byte) {
	s.wmu.Lock()
	defer s.wmu.Unlock()
	if s.conn == nil {
		return
	}
	_ = s.conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
	_, _ = s.conn.Write(p)
}

// publish stores a retained message and routes m to every matching session.
func (b *Broker) publish(m Message) {
	b.mu.Lock()
	if m.Retain {
		if len(m.Payload) == 0 {
			delete(b.retained, m.Topic) // 3.3.1-10: empty retained payload clears
		} else {
			b.retained[m.Topic] = m
		}
	}
	type target struct {
		s   *session
		qos byte
	}
	var targets []target
	for _, s := range b.sessions {
		best, hit := byte(0), false
		for f, q := range s.subs {
			if MatchTopic(f, m.Topic) {
				hit = true
				if q > best {
					best = q
				}
			}
		}
		if hit {
			targets = append(targets, target{s, best})
		}
	}
	b.mu.Unlock()
	for _, t := range targets {
		out := m
		out.Retain = false // forwarded to an existing subscriber, retain is cleared (3.3.1-9)
		out.Dup = false
		if out.QoS > t.qos {
			out.QoS = t.qos
		}
		b.deliver(t.s, out)
	}
}

// deliver writes to a live session or queues (QoS 1, persistent) for later.
func (b *Broker) deliver(s *session, m Message) {
	s.wmu.Lock()
	live := s.conn != nil
	s.wmu.Unlock()
	if !live {
		if m.QoS > 0 && !s.clean {
			b.mu.Lock()
			s.queue = append(s.queue, m)
			b.mu.Unlock()
		}
		return
	}
	var pid uint16
	if m.QoS > 0 {
		b.mu.Lock()
		s.pid++
		if s.pid == 0 {
			s.pid = 1
		}
		pid = s.pid
		b.mu.Unlock()
	}
	s.write(encodePublish(m, pid))
}

// flush replays a resumed session's QoS 1 backlog.
func (b *Broker) flush(s *session) {
	b.mu.Lock()
	q := s.queue
	s.queue = nil
	b.mu.Unlock()
	for _, m := range q {
		m.Dup = true
		b.deliver(s, m)
	}
}

// sendRetained hands the retained messages matching newly added filters to s.
func (b *Broker) sendRetained(s *session, filters []string) {
	b.mu.Lock()
	var out []Message
	for _, f := range filters {
		q := s.subs[f]
		for topic, m := range b.retained {
			if MatchTopic(f, topic) {
				r := m
				r.Retain = true
				if r.QoS > q {
					r.QoS = q
				}
				out = append(out, r)
			}
		}
	}
	b.mu.Unlock()
	for _, m := range out {
		b.deliver(s, m)
	}
}
