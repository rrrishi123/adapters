// Package mqtt is the MQTT CHANNEL adapter: a persistent broker relay.
//
// Two nodes — a locked far end (a sandbox with outbound-only egress) and the
// host that holds the witnessed wire — both connect OUT to an MQTT broker.
// Neither needs an inbound port. The broker holds the sessions; topics carry
// the wire's atoms; QoS and retain are the app routing the transports manifest
// describes ("Topic hierarchy + QoS + retain are application routing →
// adapter").
//
// The shape:
//
//	far end  ──PUBLISH──▶ broker  <prefix>/commands/<ulid>   (an ENVELOPE — a proposal, QoS 1)
//	host     ◀──deliver── broker                              (held subscription, persistent session)
//	host     ──fire─────▶ the witness (8 collector /fetch or /run)   (the CALL or CHANNEL atom, witnessed)
//	host     ──PUBLISH──▶ broker  <prefix>/receipts/<ulid>   (a thick RECEIPT, X-8-Witness inside, QoS 1, RETAINED)
//	far end  ◀──deliver── broker                              (its own held subscription — or the retained copy, later)
//	both     ──PUBLISH──▶ broker  <prefix>/presence/<node>   (retained + last-will: the held connection made visible)
//
// Reduction (README "Reduction rules"): a node's broker session is a held
// duplex connection you produce into (PUBLISH) and consume from (deliveries) —
// that is the CHANNEL atom, bidi_command, and nothing else. Unlike git
// (gitbroker), MQTT does NOT collapse CHANNEL into CALL: the connection is
// real, the broker pushes, and a receipt reaches the far end without polling.
// What rides on the channel is a dialect: an envelope names an atom (a CALL
// as httpx.Request, or a CHANNEL command as {session, method, params}) and
// the host fires it through the witness so the receipt carries X-8-Witness.
//
// Delivery: QoS 1 both ways (the broker owns delivery once it PUBACKs), receipts
// RETAINED (a far end that was away still finds its answer), persistent
// sessions (CleanSession=false: the broker queues commands while the host is
// down — the "store" in store-and-forward). Idempotency is by ULID: a command
// whose receipt already exists (retained) is never fired again. QoS 2 is
// deliberately not offered; exactly-once belongs to the receipt, not the
// transport.
//
// Trust, both directions ("policy-gated to/from"): the far end only PROPOSES;
// the host's Policy decides what fires (inbound gate) AND what the receipt may
// carry back toward a broker it does not own (egress gate: body/headers).
// Credentials never enter a topic: an envelope names an auth_slot the host
// resolves from its own slots file; Validate rejects Authorization outright.
//
// Nothing is hardcoded: broker URL, topic prefix, node name, credentials,
// collector are per-node Config (cmd/mqtt: flags with MQTT_* fallbacks).
//
// broker.go is the in-process broker the adapter owns for loopbacks and
// tests — the July "own both ends" harness, grown to speak the same dialect
// (QoS 1, retain, wildcards, will, persistent sessions) a real broker does.
package mqtt

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/rrrishi123/adapters/internal/httpx"
	"github.com/rrrishi123/adapters/trace"
)

// Schema identifiers stamped on every envelope and receipt.
const (
	EnvelopeSchema = "mqtt/envelope/v1"
	ReceiptSchema  = "mqtt/receipt/v1"
)

// The two atoms an envelope may name. Unlike git, MQTT carries both.
const (
	AtomCall    = trace.ModeCall
	AtomChannel = trace.ModeChannel
)

// ChannelOp is one CHANNEL command — the wire's bidi_command shape as the
// witness accepts it on POST /run?session=<seat>: one command on a held
// BiDi/CDP socket the host already owns. The far end never sees a ws_url;
// it names the seat and the host resolves it.
type ChannelOp struct {
	Session string          `json:"session"`          // witness seat id (e.g. "fox") — resolved host-side to the socket
	Method  string          `json:"method"`           // e.g. browsingContext.getTree
	Params  json.RawMessage `json:"params,omitempty"` // command params (defaults to {})
	ID      int             `json:"id,omitempty"`     // command id (default 1)
}

// Envelope is what the far end publishes: one proposed atom. It travels a
// broker the host may not own, so it must never carry a credential.
type Envelope struct {
	Schema string `json:"schema"`          // EnvelopeSchema
	ULID   string `json:"ulid"`            // id; also the last topic level (commands/<ulid>)
	Seq    int64  `json:"seq"`             // far end's monotonic sequence
	Agent  string `json:"agent,omitempty"` // who proposes (provenance across the boundary)
	Atom   string `json:"atom"`            // "call" | "channel" (empty defaults to call)

	Call    *httpx.Request `json:"call,omitempty"`    // atom=call: exactly the wire's http_request shape
	Channel *ChannelOp     `json:"channel,omitempty"` // atom=channel: exactly the wire's bidi_command shape (seat-addressed)

	AuthSlot   string `json:"auth_slot,omitempty"`   // name only; resolved host-side
	NotAfter   string `json:"not_after,omitempty"`   // RFC3339: a proposal that sat in the broker too long must not fire
	MaxMs      int64  `json:"max_ms,omitempty"`      // fire deadline hint; 0 = host default
	Why        string `json:"why,omitempty"`         // free text, echoed into the receipt
	ProposedAt string `json:"proposed_at,omitempty"` // RFC3339, set by the far end
}

// Validate checks the envelope is well-formed and safe to relay. It does not
// decide whether it SHOULD fire — that is Policy.
func (e *Envelope) Validate() error {
	if e.Schema != "" && e.Schema != EnvelopeSchema {
		return fmt.Errorf("mqtt: envelope schema %q, want %q", e.Schema, EnvelopeSchema)
	}
	if !SafeID(e.ULID) {
		return fmt.Errorf("mqtt: ulid %q must be [A-Za-z0-9._-]+ (it is a topic level)", e.ULID)
	}
	if e.Atom == "" {
		e.Atom = AtomCall
	}
	switch e.Atom {
	case AtomCall:
		if e.Call == nil || strings.TrimSpace(e.Call.URL) == "" {
			return errors.New("mqtt: atom=call needs call.url")
		}
		if e.Call.Method == "" {
			e.Call.Method = "GET"
		}
		for k := range e.Call.Headers {
			lk := strings.ToLower(k)
			if lk == "authorization" || lk == "proxy-authorization" || lk == "cookie" {
				return fmt.Errorf("mqtt: envelope carries a %s header — the broker is not yours; name an auth_slot instead", k)
			}
		}
	case AtomChannel:
		if e.Channel == nil || strings.TrimSpace(e.Channel.Session) == "" || strings.TrimSpace(e.Channel.Method) == "" {
			return errors.New("mqtt: atom=channel needs channel.session and channel.method")
		}
		if e.Channel.ID == 0 {
			e.Channel.ID = 1
		}
		if len(e.Channel.Params) == 0 {
			e.Channel.Params = json.RawMessage(`{}`)
		}
	default:
		return fmt.Errorf("mqtt: atom %q is neither %q nor %q", e.Atom, AtomCall, AtomChannel)
	}
	if e.NotAfter != "" {
		if _, err := time.Parse(time.RFC3339, e.NotAfter); err != nil {
			return fmt.Errorf("mqtt: not_after %q is not RFC3339", e.NotAfter)
		}
	}
	return nil
}

// Expired reports whether the envelope's not_after is in the past at now.
func (e *Envelope) Expired(now time.Time) bool {
	if e.NotAfter == "" {
		return false
	}
	t, err := time.Parse(time.RFC3339, e.NotAfter)
	return err == nil && now.After(t)
}

// SafeID reports whether s can be a single topic level and a file name.
func SafeID(s string) bool {
	if s == "" || len(s) > 128 || s == "." || s == ".." {
		return false
	}
	for _, r := range s {
		ok := r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' || r == '.' || r == '_' || r == '-'
		if !ok {
			return false
		}
	}
	return true
}

// Egress is the outbound gate: what a receipt may carry back toward the
// broker. nil in a Decision means everything (body capped, headers included).
type Egress struct {
	Body    bool `json:"body"`    // include the response body (capped) — false = digest and length only
	Headers bool `json:"headers"` // include response headers
}

// Decision is the host's verdict on one envelope.
type Decision struct {
	Fired  bool    `json:"fired"`            // true only if the atom actually left this machine
	Policy string  `json:"policy,omitempty"` // which gate decided (name)
	Reason string  `json:"reason,omitempty"` // why it did not fire, or why it did
	Egress *Egress `json:"egress,omitempty"` // outbound gate applied to the receipt
}

// CallEcho is the part of a fired CALL a receipt may echo. Headers are
// deliberately absent: after auth_slot resolution they hold a credential.
type CallEcho struct {
	Method string `json:"method"`
	URL    string `json:"url"`
}

// ChannelEcho is the part of a fired CHANNEL command a receipt echoes.
type ChannelEcho struct {
	Session string `json:"session"`
	Method  string `json:"method"`
}

// Witness is the reafference the 8 collector stamps on every response
// (X-8-Witness / X-8-Ledger-Id / X-8-DMs, and X-8-Ledger on /run), carried
// back verbatim.
type Witness struct {
	Line     string `json:"line,omitempty"`      // X-8-Witness
	LedgerID string `json:"ledger_id,omitempty"` // X-8-Ledger-Id
	Ledger   string `json:"ledger,omitempty"`    // X-8-Ledger
	DMs      string `json:"dms,omitempty"`       // X-8-DMs
}

// HostID says who processed the envelope, for provenance on the far end.
type HostID struct {
	Node  string `json:"node,omitempty"`  // Config.Node (the MQTT client id)
	Actor string `json:"actor,omitempty"` // X-8-Actor declared on the fire
	Host  string `json:"host,omitempty"`  // os hostname
}

// Route is how the receipt itself travelled: the app-routing knobs, made visible.
type Route struct {
	Topic  string `json:"topic"`  // <prefix>/receipts/<ulid>
	QoS    byte   `json:"qos"`    // delivery guarantee used
	Retain bool   `json:"retain"` // true: the broker keeps it for a far end that was away
}

// Receipt is what the host publishes back: thick enough that the far end
// learns the outcome from it alone.
type Receipt struct {
	Schema   string `json:"schema"`   // ReceiptSchema
	Contract string `json:"contract"` // trace.Version — the wire contract this receipt speaks
	ULID     string `json:"ulid"`
	Seq      int64  `json:"seq"`
	Agent    string `json:"agent,omitempty"`
	Atom     string `json:"atom"`
	Why      string `json:"why,omitempty"`

	Decision Decision     `json:"decision"`
	Call     *CallEcho    `json:"call,omitempty"`
	Channel  *ChannelEcho `json:"channel,omitempty"`

	Status     int               `json:"status,omitempty"`      // HTTP status of the afferent leg at the witness
	LatencyMs  int64             `json:"latency_ms,omitempty"`  // measured at the host
	BodyLen    int               `json:"body_len,omitempty"`    // full length before any cap
	BodyDigest string            `json:"body_digest,omitempty"` // sha256 hex of the FULL body
	Body       string            `json:"body,omitempty"`        // capped at Config.MaxBody; absent when egress.body=false
	Truncated  bool              `json:"truncated,omitempty"`
	Headers    map[string]string `json:"headers,omitempty"` // response headers (first value each); absent when egress.headers=false

	Witness Witness `json:"witness"`
	Error   string  `json:"error,omitempty"`

	ProcessedAt string `json:"processed_at"` // RFC3339
	Host        HostID `json:"host"`
	Route       Route  `json:"route"`
}

// Digest is the receipt's body digest: sha256 hex of the full body.
func Digest(b []byte) string {
	s := sha256.Sum256(b)
	return hex.EncodeToString(s[:])
}
