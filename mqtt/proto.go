package mqtt

// MQTT 3.1.1 framing — the one thing every MQTT byte rides on: a fixed header
// (type nibble + flags), a variable-length "remaining length", then the body.
// Pure stdlib; shared by the client and the in-process broker so the adapter
// owns both ends of the shape it claims.

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"strings"
)

// Control-packet type values (the high nibble of the fixed header).
const (
	typeCONNECT     = 0x10
	typeCONNACK     = 0x20
	typePUBLISH     = 0x30
	typePUBACK      = 0x40
	typeSUBSCRIBE   = 0x80
	typeSUBACK      = 0x90
	typeUNSUBSCRIBE = 0xA0
	typeUNSUBACK    = 0xB0
	typePINGREQ     = 0xC0
	typePINGRESP    = 0xD0
	typeDISCONNECT  = 0xE0
)

// SUBSCRIBE / UNSUBSCRIBE carry mandatory flag bits 0010 (3.8.1, 3.10.1).
const (
	pktSUBSCRIBE   = typeSUBSCRIBE | 0x02
	pktUNSUBSCRIBE = typeUNSUBSCRIBE | 0x02
)

// CONNACK return codes we care about.
const (
	connAccepted     = 0x00
	connBadProtocol  = 0x01
	connIDRejected   = 0x02
	connBadAuth      = 0x04
	connNotAuthorize = 0x05
)

// Message is one MQTT application message as the adapter sees it: the topic
// (routing), the payload (an atom's bytes), and the two delivery knobs the
// transports manifest calls "app routing": QoS and retain.
type Message struct {
	Topic   string
	Payload []byte
	QoS     byte // 0 = fire-and-forget, 1 = at-least-once (PUBACK). QoS 2 is not offered (see package doc).
	Retain  bool // broker keeps the last message per topic and hands it to late subscribers
	Dup     bool // redelivery flag (set by the sender on retransmit)
}

// packet frames a control packet: fixed-header byte, remaining length, body.
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
			return 0, errors.New("mqtt: remaining length malformed")
		}
	}
}

// putString appends a 2-byte-length-prefixed UTF-8 string (MQTT's string form).
func putString(b []byte, s string) []byte {
	var l [2]byte
	binary.BigEndian.PutUint16(l[:], uint16(len(s)))
	return append(append(b, l[:]...), s...)
}

// putBytes appends a 2-byte-length-prefixed binary field (password, will payload).
func putBytes(b []byte, p []byte) []byte {
	var l [2]byte
	binary.BigEndian.PutUint16(l[:], uint16(len(p)))
	return append(append(b, l[:]...), p...)
}

func putUint16(b []byte, v uint16) []byte { return append(b, byte(v>>8), byte(v)) }

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

func getUint16(p []byte) (v uint16, rest []byte, ok bool) {
	if len(p) < 2 {
		return 0, p, false
	}
	return binary.BigEndian.Uint16(p), p[2:], true
}

// readPacket reads one full control packet: header byte + body. maxBody caps
// the remaining length so a hostile peer cannot ask for gigabytes.
func readPacket(r interface {
	io.ByteReader
	io.Reader
}, maxBody int) (header byte, body []byte, err error) {
	header, err = r.ReadByte()
	if err != nil {
		return 0, nil, err
	}
	rl, err := readRemLen(r)
	if err != nil {
		return 0, nil, err
	}
	if maxBody > 0 && rl > maxBody {
		return 0, nil, fmt.Errorf("mqtt: packet of %d bytes exceeds cap %d", rl, maxBody)
	}
	body = make([]byte, rl)
	_, err = io.ReadFull(r, body)
	return header, body, err
}

// encodePublish builds a PUBLISH packet. pid is only written for QoS > 0.
func encodePublish(m Message, pid uint16) []byte {
	h := byte(typePUBLISH) | (m.QoS&0x3)<<1
	if m.Retain {
		h |= 0x01
	}
	if m.Dup {
		h |= 0x08
	}
	body := putString(nil, m.Topic)
	if m.QoS > 0 {
		body = putUint16(body, pid)
	}
	return packet(h, append(body, m.Payload...))
}

// decodePublish splits a PUBLISH body by the flags in its header.
func decodePublish(header byte, body []byte) (m Message, pid uint16, err error) {
	m.QoS = (header >> 1) & 0x3
	m.Retain = header&0x01 != 0
	m.Dup = header&0x08 != 0
	topic, rest, ok := getString(body)
	if !ok {
		return m, 0, errors.New("mqtt: PUBLISH without a topic")
	}
	if m.QoS > 0 {
		if pid, rest, ok = getUint16(rest); !ok {
			return m, 0, errors.New("mqtt: PUBLISH QoS>0 without a packet id")
		}
	}
	m.Topic = topic
	m.Payload = append([]byte{}, rest...)
	return m, pid, nil
}

// --- topic routing (the "app routing" the manifest speaks of) ---

// ValidTopic reports whether s may be PUBLISHed to: non-empty, no wildcards.
func ValidTopic(s string) bool {
	return s != "" && !strings.ContainsAny(s, "+#\x00")
}

// ValidFilter reports whether a subscription filter is well-formed:
// '+' only as a whole level, '#' only as the whole last level.
func ValidFilter(f string) bool {
	if f == "" || strings.ContainsRune(f, 0) {
		return false
	}
	levels := strings.Split(f, "/")
	for i, l := range levels {
		switch {
		case l == "#":
			if i != len(levels)-1 {
				return false
			}
		case strings.ContainsAny(l, "+#"):
			if l != "+" {
				return false
			}
		}
	}
	return true
}

// MatchTopic implements 4.7 topic-filter matching: '+' matches exactly one
// level, '#' matches the remainder (including zero levels). Topics beginning
// with '$' are never matched by a filter that starts with a wildcard (4.7.2).
func MatchTopic(filter, topic string) bool {
	if strings.HasPrefix(topic, "$") && (strings.HasPrefix(filter, "+") || strings.HasPrefix(filter, "#")) {
		return false
	}
	fl := strings.Split(filter, "/")
	tl := strings.Split(topic, "/")
	for i, f := range fl {
		if f == "#" {
			return true
		}
		if i >= len(tl) {
			return false
		}
		if f != "+" && f != tl[i] {
			return false
		}
	}
	return len(fl) == len(tl)
}
