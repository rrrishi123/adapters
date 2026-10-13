package gitbroker

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/rrrishi123/adapters/internal/guard"
	"github.com/rrrishi123/adapters/internal/httpx"
	"github.com/rrrishi123/adapters/trace"
)

// Poller reads envelopes from the bridge, decides, fires, and commits receipts.
type Poller struct {
	Bridge *Bridge
	Firer  Firer  // nil = CollectorFirer from Bridge config
	Policy Policy // nil = Closed (fail shut); loopbacks set AllowAll explicitly
	Log    *log.Logger
	Now    func() time.Time // nil = time.Now
}

// Report is what one poll did.
type Report struct {
	Mode      string   `json:"mode"`
	Processed []string `json:"processed"`
	Fired     int      `json:"fired"`
	Refused   int      `json:"refused"`
	Errors    int      `json:"errors"`
	Halted    bool     `json:"halted,omitempty"` // state/halt present: the poll stopped before a fire, unanswered envelopes wait
	Commit    string   `json:"commit,omitempty"`
	SyncError string   `json:"sync_error,omitempty"`
}

// ErrHalted is returned by process when the far end's HALT appeared
// immediately before the fire; the envelope stays unanswered.
var ErrHalted = fmt.Errorf("gitbroker: HALTED — %s present, refusing to fire", filepath.Join("state", "halt"))

// Check verifies the poller's safety configuration before any poll: a
// FilePolicy must live outside the bridge checkout (G1). Once and Run call it;
// a failure here is fatal, not a refusal receipt — the relay must not start.
func (p *Poller) Check() error {
	if p.Bridge == nil {
		return fmt.Errorf("gitbroker: Poller.Bridge is nil")
	}
	if fp, ok := p.Policy.(FilePolicy); ok {
		if _, err := guard.Outside(fp.Path, p.Bridge.cfg.Dir); err != nil {
			return fmt.Errorf("gitbroker: policy: %w", err)
		}
	}
	if fp, ok := p.Policy.(*FilePolicy); ok && fp != nil {
		if _, err := guard.Outside(fp.Path, p.Bridge.cfg.Dir); err != nil {
			return fmt.Errorf("gitbroker: policy: %w", err)
		}
	}
	return nil
}

func (p *Poller) firer() Firer {
	if p.Firer != nil {
		return p.Firer
	}
	c := p.Bridge.cfg
	return &CollectorFirer{Collector: c.Collector, Actor: c.Actor}
}

func (p *Poller) policy() Policy {
	if p.Policy != nil {
		return p.Policy
	}
	return Closed
}

func (p *Poller) now() time.Time {
	if p.Now != nil {
		return p.Now()
	}
	return time.Now()
}

func (p *Poller) logf(format string, a ...any) {
	if p.Log != nil {
		p.Log.Printf(format, a...)
	}
}

// Once performs one poll: pull, process every envelope without a receipt (in
// name order — ULIDs sort by time), commit the receipts, push. Processing is
// idempotent: an envelope with an existing receipt is never touched again, so
// a crash between fire and push is at worst one duplicate fire, never a lost
// receipt.
func (p *Poller) Once(ctx context.Context) (Report, error) {
	if err := p.Check(); err != nil {
		return Report{}, err
	}
	rep := Report{Mode: p.Bridge.Mode()}
	if err := p.Bridge.Pull(); err != nil {
		rep.SyncError = err.Error()
		p.logf("%v (continuing on local state)", err)
	}
	entries, err := os.ReadDir(p.Bridge.commandsPath())
	if err != nil {
		return rep, err
	}
	var names []string
	for _, e := range entries {
		if !e.IsDir() && strings.HasSuffix(e.Name(), ".json") {
			names = append(names, e.Name())
		}
	}
	sort.Strings(names)
	for _, fn := range names {
		if ctx.Err() != nil {
			break
		}
		ulid := strings.TrimSuffix(fn, ".json")
		rpath := filepath.Join(p.Bridge.receiptsPath(), fn)
		if _, err := os.Stat(rpath); err == nil {
			continue // already answered — idempotent
		}
		// HALT — the far end's revocation, checked before EVERY envelope and
		// again inside process immediately before the fire. A halted poll
		// leaves the rest unanswered so they fire once the halt is lifted.
		if p.Bridge.Halted() {
			rep.Halted = true
			p.logf("HALTED — %s present, refusing to fire (%d envelope(s) left unanswered)", p.Bridge.haltPath(), len(names))
			break
		}
		rec, err := p.process(ctx, ulid, filepath.Join(p.Bridge.commandsPath(), fn))
		if err == ErrHalted {
			rep.Halted = true
			p.logf("HALTED — %s appeared before the fire of %s; left unanswered", p.Bridge.haltPath(), ulid)
			break
		}
		if err := writeJSONAtomic(rpath, rec); err != nil {
			return rep, fmt.Errorf("gitbroker: write receipt %s: %w", ulid, err)
		}
		rep.Processed = append(rep.Processed, ulid)
		switch {
		case rec.Decision.Fired && rec.Error == "":
			rep.Fired++
		case rec.Decision.Fired:
			rep.Errors++
		default:
			rep.Refused++
		}
		p.logf("%s %s %s → fired=%v status=%d %s", ulid, rec.Call.Method, rec.Call.URL, rec.Decision.Fired, rec.Status, firstNonEmpty(rec.Error, rec.Decision.Reason, rec.Witness.Line))
	}
	if len(rep.Processed) > 0 {
		sha, err := p.Bridge.CommitReceipts(rep.Processed)
		if err != nil {
			return rep, err
		}
		rep.Commit = sha
		if err := p.Bridge.Push(); err != nil {
			rep.SyncError = err.Error()
			p.logf("%v (receipts are committed locally; next poll retries the push)", err)
		}
	}
	return rep, nil
}

// Run polls at Config.PollInterval until ctx is done.
func (p *Poller) Run(ctx context.Context) error {
	if err := p.Check(); err != nil {
		return err
	}
	iv := p.Bridge.cfg.PollInterval
	if iv <= 0 {
		return fmt.Errorf("gitbroker: Config.PollInterval must be > 0 for Run")
	}
	t := time.NewTicker(iv)
	defer t.Stop()
	for {
		if _, err := p.Once(ctx); err != nil {
			p.logf("poll: %v", err)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-t.C:
		}
	}
}

// process turns one envelope file into a receipt. Every path — unparseable,
// invalid, expired, refused, failed, fired — yields a receipt, because the far
// end has no other way to learn what happened. The one exception is HALT
// appearing immediately before the fire: then it returns ErrHalted and no
// receipt, so the envelope is answered after the halt is lifted.
func (p *Poller) process(ctx context.Context, ulid, path string) (*Receipt, error) {
	host, _ := os.Hostname()
	rec := &Receipt{
		Schema:      ReceiptSchema,
		Contract:    trace.Version,
		ULID:        ulid,
		Atom:        AtomCall,
		Call:        &CallEcho{},
		ProcessedAt: p.now().UTC().Format(time.RFC3339),
		Poller:      PollerID{Actor: p.Bridge.cfg.Actor, Host: host},
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		rec.Error = "unreadable envelope: " + err.Error()
		return rec, nil
	}
	var env Envelope
	if err := json.Unmarshal(raw, &env); err != nil {
		rec.Error = "unparseable envelope: " + err.Error()
		return rec, nil
	}
	rec.Seq, rec.Agent, rec.Why = env.Seq, env.Agent, env.Why
	rec.Call = &CallEcho{Method: env.Call.Method, URL: env.Call.URL}
	if err := env.Validate(); err != nil {
		rec.Decision = Decision{Fired: false, Policy: "validate", Reason: err.Error()}
		return rec, nil
	}
	rec.Call.Method = env.Call.Method // Validate may have defaulted it
	if env.ULID != "" && env.ULID != ulid {
		rec.Decision = Decision{Fired: false, Policy: "validate", Reason: fmt.Sprintf("envelope ulid %q does not match file name %q", env.ULID, ulid)}
		return rec, nil
	}
	if env.Expired(p.now()) {
		rec.Decision = Decision{Fired: false, Policy: "expiry", Reason: "expired (not_after " + env.NotAfter + ")"}
		return rec, nil
	}
	// the authoritative side decides — the far end only proposed
	d := p.policy().Decide(&env)
	rec.Decision = d
	if !d.Fired {
		return rec, nil
	}
	call := env.Call
	authenticated := env.AuthSlot != ""
	if authenticated {
		auth, err := p.resolveSlot(env.AuthSlot)
		if err != nil {
			rec.Decision = Decision{Fired: false, Policy: "auth-slot", Reason: err.Error()}
			return rec, nil
		}
		auth.Apply(&call) // the credential exists only here, on this side, in memory
		rec.Authenticated = true
	}
	timeout := p.Bridge.cfg.FireTimeout
	if env.MaxMs > 0 {
		timeout = time.Duration(env.MaxMs) * time.Millisecond
	}
	// HALT — checked immediately before the fire, as poll.py did
	if p.Bridge.Halted() {
		return nil, ErrHalted
	}
	fctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	out, err := p.firer().Fire(fctx, call)
	if err != nil {
		rec.Error = "fire failed: " + err.Error()
		return rec, nil
	}
	// G3: the receipt is a public artifact — egress-gated, headers allowlisted
	egress := d.Egress
	if egress == nil {
		egress = guard.DefaultEgress()
	}
	rec.Status = out.Status
	rec.LatencyMs = out.Latency.Milliseconds()
	rec.BodyLen = len(out.Body)
	rec.BodyDigest = Digest(out.Body)
	if egress.PublishBody(authenticated) {
		if max := p.Bridge.cfg.MaxBody; len(out.Body) > max {
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
	return rec, nil
}

// resolveSlot maps an auth_slot NAME to a credential from the host-side slots
// file ({"<slot>": {"type":"bearer","key":"..."}, ...}). The file lives
// OUTSIDE the bridge checkout (Config.Slots; Open refuses a path inside it) —
// it is the one thing that must never be a commit away from public.
func (p *Poller) resolveSlot(slot string) (httpx.Auth, error) {
	if p.Bridge.cfg.Slots == "" {
		return httpx.Auth{}, fmt.Errorf("auth_slot %q: no slots file configured on the host (-slots / GITBROKER_SLOTS)", slot)
	}
	b, err := os.ReadFile(p.Bridge.cfg.Slots)
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

func writeJSONAtomic(path string, v any) error {
	b, err := json.MarshalIndent(v, "", " ")
	if err != nil {
		return err
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, append(b, '\n'), 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

func firstNonEmpty(xs ...string) string {
	for _, x := range xs {
		if x != "" {
			return x
		}
	}
	return ""
}

// ReadReceipt loads one receipt from the bridge — the far end's read side.
func ReadReceipt(r io.Reader) (*Receipt, error) {
	var rec Receipt
	if err := json.NewDecoder(r).Decode(&rec); err != nil {
		return nil, err
	}
	return &rec, nil
}
