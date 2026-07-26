// Package mqtt is the MQTT adapter's own-both-ends conformance harness: a
// minimal-but-real MQTT 3.1.1 broker and client, pure stdlib, no external broker.
//
// It exists to PROVE the transport shape the wire delegates here, end to end:
//
//	subscribe = CHANNEL — a long-lived broker connection whose messages arrive
//	                      as frames (maps to bidi_command)
//	publish   = CALL     — fire one message into a topic (maps to http_request)
//
// We are the broker AND the client; total validation within the layer actually
// used, the same way http-mcp's websocket conformance stands up its own RFC6455
// echo server and drives wsx against it.
//
// Deliberately NOT a production broker: QoS 0 only, exact-topic routing, no
// retain / last-will / keep-alive. Those are the "capability-shaped" bits
// (capabilities.json) a real adapter layers on; none of them change the
// CHANNEL/CALL mapping this file proves.
package mqtt

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"sync"
	"time"
)

// MQTT 3.1.1 control-packet type nibbles (high 4 bits of the fixed header).
const (
	pktCONNECT   = 0x10
	pktCONNACK   = 0x20
	pktPUBLISH   = 0x30
	pktSUBSCRIBE = 0x80
	pktSUBACK    = 0x90
)

// packet frames a control packet: fixed-header byte, variable-length remaining
// length, then body. This is the one framing rule MQTT rides on.
func packet(header byte, body []byte) []byte {
	var rl []byte
	n := len(body)
	for {
		b := byte(n % 128)
		n /= 128
		if n > 0 {
			b |= 0x80 // continuation bit
		}
		rl = append(rl, b)
		if n == 0 {
			break
		}
	}
	out := append([]byte{header}, rl...)
	return append(out, body...)
}

// readRemLen decodes MQTT's variable-length "remaining length" field.
func readRemLen(r io.ByteReader) (int, error) {
	mult, val := 1, 0
	for {
		b, err := r.ReadByte()
		if err != nil {
			return 0, err
		}
		val += int(b&0x7f) * mult
		if b&0x80 == 0 {
			return val, nil
		}
		if mult *= 128; mult > 128*128*128 {
			return 0, fmt.Errorf("mqtt: remaining length malformed")
		}
	}
}

// putString appends a 2-byte-length-prefixed UTF-8 string (MQTT's string form).
func putString(b []byte, s string) []byte {
	var l [2]byte
	binary.BigEndian.PutUint16(l[:], uint16(len(s)))
	return append(append(b, l[:]...), s...)
}

// getString reads one length-prefixed string, returning the rest of the buffer.
func getString(p []byte) (s string, rest []byte, ok bool) {
	if len(p) < 2 {
		return "", p, false
	}
	n := int(binary.BigEndian.Uint16(p))
	if len(p) < 2+n {
		return "", p, false
	}
	return string(p[2 : 2+n]), p[2+n:], true
}

// readPacket reads one full control packet: header byte + body.
func readPacket(r *bufio.Reader) (header byte, body []byte, err error) {
	header, err = r.ReadByte()
	if err != nil {
		return 0, nil, err
	}
	rl, err := readRemLen(r)
	if err != nil {
		return 0, nil, err
	}
	body = make([]byte, rl)
	_, err = io.ReadFull(r, body)
	return header, body, err
}

// --- Broker: we own the channel endpoint ---

// Broker is a minimal MQTT broker that routes PUBLISH to exact-topic subscribers.
type Broker struct {
	ln   net.Listener
	mu   sync.Mutex
	subs map[string][]net.Conn // topic -> subscriber connections
}

// NewBroker binds a loopback broker on an ephemeral port and starts serving.
func NewBroker() (*Broker, error) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, err
	}
	b := &Broker{ln: ln, subs: map[string][]net.Conn{}}
	go b.serve()
	return b, nil
}

func (b *Broker) Addr() string { return b.ln.Addr().String() }
func (b *Broker) Close() error { return b.ln.Close() }

func (b *Broker) serve() {
	for {
		conn, err := b.ln.Accept()
		if err != nil {
			return
		}
		go b.handle(conn)
	}
}

func (b *Broker) handle(conn net.Conn) {
	defer conn.Close()
	r := bufio.NewReader(conn)
	for {
		header, body, err := readPacket(r)
		if err != nil {
			return
		}
		switch header & 0xf0 {
		case pktCONNECT:
			_, _ = conn.Write(packet(pktCONNACK, []byte{0x00, 0x00})) // accepted

		case pktSUBSCRIBE:
			pid := body[:2] // packet id echoed in SUBACK
			rest := body[2:]
			var codes []byte
			for len(rest) > 0 {
				topic, r2, ok := getString(rest)
				if !ok || len(r2) < 1 {
					break
				}
				rest = r2[1:] // consume the per-topic requested-QoS byte
				b.mu.Lock()
				b.subs[topic] = append(b.subs[topic], conn)
				b.mu.Unlock()
				codes = append(codes, 0x00) // QoS 0 granted
			}
			_, _ = conn.Write(packet(pktSUBACK, append(append([]byte{}, pid...), codes...)))

		case pktPUBLISH:
			topic, payload, ok := getString(body)
			if !ok {
				return
			}
			b.mu.Lock()
			conns := append([]net.Conn{}, b.subs[topic]...)
			b.mu.Unlock()
			out := packet(pktPUBLISH, append(putString(nil, topic), payload...))
			for _, sc := range conns {
				_, _ = sc.Write(out)
			}
		}
	}
}

// --- Client: we drive both the CHANNEL (subscribe) and the CALL (publish) ---

// Client is a minimal MQTT client. Subscribe opens a CHANNEL (a stream of frames
// for a topic); Publish is a CALL (one message into a topic).
type Client struct {
	conn   net.Conn
	r      *bufio.Reader
	pid    uint16
	smu    sync.Mutex
	subs   map[string]chan []byte
	suback chan struct{}
}

// Dial connects, sends CONNECT, waits for CONNACK, and starts the read loop.
func Dial(addr, clientID string) (*Client, error) {
	conn, err := net.DialTimeout("tcp", addr, 3*time.Second)
	if err != nil {
		return nil, err
	}
	c := &Client{
		conn:   conn,
		r:      bufio.NewReader(conn),
		subs:   map[string]chan []byte{},
		suback: make(chan struct{}, 1),
	}
	body := putString(nil, "MQTT")     // protocol name
	body = append(body, 0x04)          // protocol level 4 = MQTT 3.1.1
	body = append(body, 0x02)          // connect flags: clean session
	body = append(body, 0x00, 0x3c)    // keep-alive 60s (unused here)
	body = putString(body, clientID)   // payload: client id
	if _, err := conn.Write(packet(pktCONNECT, body)); err != nil {
		conn.Close()
		return nil, err
	}
	header, _, err := readPacket(c.r)
	if err != nil || header&0xf0 != pktCONNACK {
		conn.Close()
		return nil, fmt.Errorf("mqtt: no CONNACK (err=%v header=%#x)", err, header)
	}
	go c.readLoop()
	return c, nil
}

func (c *Client) readLoop() {
	for {
		header, body, err := readPacket(c.r)
		if err != nil {
			return
		}
		switch header & 0xf0 {
		case pktPUBLISH:
			topic, payload, ok := getString(body)
			if !ok {
				continue
			}
			c.smu.Lock()
			ch := c.subs[topic]
			c.smu.Unlock()
			if ch != nil {
				select {
				case ch <- append([]byte{}, payload...):
				default: // drop if the consumer is slow — QoS 0 semantics
				}
			}
		case pktSUBACK:
			select {
			case c.suback <- struct{}{}:
			default:
			}
		}
	}
}

// Subscribe opens the CHANNEL for a topic and blocks until SUBACK confirms it.
func (c *Client) Subscribe(topic string) (<-chan []byte, error) {
	ch := make(chan []byte, 8)
	c.smu.Lock()
	c.subs[topic] = ch
	c.pid++
	pid := c.pid
	c.smu.Unlock()

	body := []byte{byte(pid >> 8), byte(pid)}
	body = putString(body, topic)
	body = append(body, 0x00) // requested QoS 0
	if _, err := c.conn.Write(packet(pktSUBSCRIBE, body)); err != nil {
		return nil, err
	}
	select {
	case <-c.suback:
		return ch, nil
	case <-time.After(3 * time.Second):
		return nil, fmt.Errorf("mqtt: no SUBACK for %q", topic)
	}
}

// Publish fires one message into a topic — the CALL.
func (c *Client) Publish(topic string, payload []byte) error {
	body := putString(nil, topic)
	body = append(body, payload...)
	_, err := c.conn.Write(packet(pktPUBLISH, body))
	return err
}

// Close closes the client connection.
func (c *Client) Close() error { return c.conn.Close() }
