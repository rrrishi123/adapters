// Package gitbroker is the git-broker CALL adapter: a store-and-forward relay
// that lets a locked far end (a sandbox with no inbound and git-only egress,
// e.g. a claude-web container) reach this machine's witnessed wire.
//
// The shape:
//
//	far end   ──commit──▶  bridge repo  commands/<ulid>.json   (an ENVELOPE — a proposal)
//	poller    ◀──pull────  bridge repo
//	poller    ──fire─────▶ the witness (8 collector /fetch)     (the CALL atom, witnessed)
//	poller    ──commit──▶  bridge repo  receipts/<ulid>.json    (a thick RECEIPT, X-8-Witness inside)
//	far end   ◀──pull────  bridge repo
//
// Reduction (README "Reduction rules"): git is a queue, and a queue is
// CALL-post + CALL-poll for classification — so this dialect reduces to the
// CALL atom and nothing else. Git collapses CHANNEL into CALL: there is no held
// connection across a commit, so an envelope carries exactly one httpx.Request
// (the wire's CALL shape) and a receipt carries the whole afferent leg. That is
// why receipts are THICK — status, headers, body (capped), digest, latency and
// the X-8-Witness reafference line — the far end learns the outcome from the
// receipt alone or the channel is theater with extra steps.
//
// Trust: the far end only PROPOSES. The authoritative side (this poller) decides
// what fires through the Policy hook (policy.go). Credentials never cross the
// bridge: an envelope names an auth_slot, the poller resolves it host-side
// (state/slots.json) and the receipt never echoes request headers.
//
// poll.py in this directory is the August prototype this package formalizes
// (same bridge layout; the Go adapter is the shippable one).
package gitbroker

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/rrrishi123/adapters/internal/httpx"
	"github.com/rrrishi123/adapters/trace"
)

// Schema identifiers stamped on every envelope and receipt. A reader that sees
// a different major refuses; see envelope.schema.json / receipt.schema.json.
const (
	EnvelopeSchema = "gitbroker/envelope/v1"
	ReceiptSchema  = "gitbroker/receipt/v1"
)

// AtomCall is the only atom a git envelope can carry (see package doc).
const AtomCall = trace.ModeCall

// Envelope is what the far end commits: one proposed CALL. It is a PUBLIC
// artifact (the bridge branch may be readable by anyone), so it must never
// carry a credential — Validate rejects an Authorization header outright and
// auth_slot is a NAME the host resolves.
type Envelope struct {
	Schema string `json:"schema"`          // EnvelopeSchema
	ULID   string `json:"ulid"`            // id; also the file name commands/<ulid>.json
	Seq    int64  `json:"seq"`             // far end's monotonic sequence (gap → the far end can refuse)
	Agent  string `json:"agent,omitempty"` // who proposes (provenance across the boundary)
	Atom   string `json:"atom"`            // must be "call" (empty defaults to call)

	Call httpx.Request `json:"call"` // THE CALL ATOM — exactly the wire's http_request shape

	AuthSlot   string `json:"auth_slot,omitempty"`   // name only; resolved host-side, never a value
	NotAfter   string `json:"not_after,omitempty"`   // RFC3339: a proposal that sat too long in git must not fire
	MaxMs      int64  `json:"max_ms,omitempty"`      // fire deadline hint; 0 = poller default
	Why        string `json:"why,omitempty"`         // free text, echoed into the receipt
	ProposedAt string `json:"proposed_at,omitempty"` // RFC3339, set by the far end
}

// ErrNotCall is returned for an envelope whose atom is not "call".
var ErrNotCall = errors.New("gitbroker: atom must be \"call\" — git collapses CHANNEL into CALL; wrap a channel op as a CALL to the witness (POST /run)")

// Validate checks the envelope is well-formed and safe to relay. It does not
// decide whether it SHOULD fire — that is Policy.
func (e *Envelope) Validate() error {
	if e.Schema != "" && e.Schema != EnvelopeSchema {
		return fmt.Errorf("gitbroker: envelope schema %q, want %q", e.Schema, EnvelopeSchema)
	}
	if !safeID(e.ULID) {
		return fmt.Errorf("gitbroker: ulid %q must be [A-Za-z0-9._-]+ (it names a file)", e.ULID)
	}
	if e.Atom == "" {
		e.Atom = AtomCall
	}
	if e.Atom != AtomCall {
		return ErrNotCall
	}
	if strings.TrimSpace(e.Call.URL) == "" {
		return errors.New("gitbroker: call.url is required")
	}
	if e.Call.Method == "" {
		e.Call.Method = "GET"
	}
	for k := range e.Call.Headers {
		lk := strings.ToLower(k)
		if lk == "authorization" || lk == "proxy-authorization" || lk == "cookie" {
			return fmt.Errorf("gitbroker: envelope carries a %s header — the bridge is public; name an auth_slot instead", k)
		}
	}
	if e.NotAfter != "" {
		if _, err := time.Parse(time.RFC3339, e.NotAfter); err != nil {
			return fmt.Errorf("gitbroker: not_after %q is not RFC3339", e.NotAfter)
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

func safeID(s string) bool {
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

// Decision is the authoritative side's verdict on one envelope.
type Decision struct {
	Fired  bool   `json:"fired"`            // true only if the CALL actually left this machine
	Policy string `json:"policy,omitempty"` // which policy decided (name)
	Reason string `json:"reason,omitempty"` // why it did not fire, or why it did
}

// CallEcho is the part of the fired CALL a receipt may echo. Headers are
// deliberately absent: after auth_slot resolution they hold a credential.
type CallEcho struct {
	Method string `json:"method"`
	URL    string `json:"url"`
}

// Witness is the reafference the 8 collector stamps on every response
// (X-8-Witness / X-8-Ledger-Id / X-8-DMs, and X-8-Ledger on /run). It is the
// afferent leg's proof that the act was seen, carried back verbatim.
type Witness struct {
	Line     string `json:"line,omitempty"`      // X-8-Witness
	LedgerID string `json:"ledger_id,omitempty"` // X-8-Ledger-Id
	Ledger   string `json:"ledger,omitempty"`    // X-8-Ledger (frames seq, when present)
	DMs      string `json:"dms,omitempty"`       // X-8-DMs (ms to first byte at the witness)
}

// PollerID says who processed the envelope, for provenance on the far end.
type PollerID struct {
	Actor string `json:"actor,omitempty"` // X-8-Actor declared on the fire
	Host  string `json:"host,omitempty"`
}

// Receipt is what the poller commits back: thick enough that the far end
// learns the outcome without any other channel.
type Receipt struct {
	Schema   string `json:"schema"`   // ReceiptSchema
	Contract string `json:"contract"` // trace.Version — the wire contract this receipt speaks
	ULID     string `json:"ulid"`
	Seq      int64  `json:"seq"`
	Agent    string `json:"agent,omitempty"`
	Atom     string `json:"atom"`
	Why      string `json:"why,omitempty"`

	Decision Decision  `json:"decision"`
	Call     *CallEcho `json:"call,omitempty"`

	Status     int               `json:"status,omitempty"`      // HTTP status of the afferent leg
	LatencyMs  int64             `json:"latency_ms,omitempty"`  // measured at the poller
	BodyLen    int               `json:"body_len,omitempty"`    // full length before the cap
	BodyDigest string            `json:"body_digest,omitempty"` // sha256 hex of the FULL body
	Body       string            `json:"body,omitempty"`        // capped at Config.MaxBody
	Truncated  bool              `json:"truncated,omitempty"`
	Headers    map[string]string `json:"headers,omitempty"` // response headers (first value each)

	Witness Witness `json:"witness"`
	Error   string  `json:"error,omitempty"`

	ProcessedAt string   `json:"processed_at"` // RFC3339
	Poller      PollerID `json:"poller"`
}

// Digest is the receipt's body digest: sha256 hex of the full body.
func Digest(b []byte) string {
	s := sha256.Sum256(b)
	return hex.EncodeToString(s[:])
}
