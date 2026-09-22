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

	"github.com/rrrishi123/adapters/internal/httpx"
	"github.com/rrrishi123/adapters/trace"
)

// Poller reads envelopes from the bridge, decides, fires, and commits receipts.
type Poller struct {
	Bridge *Bridge
	Firer  Firer  // nil = CollectorFirer from Bridge config
	Policy Policy // nil = AllowAll (loopback default; set a real one in production)
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
	Commit    string   `json:"commit,omitempty"`
	SyncError string   `json:"sync_error,omitempty"`
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
	return AllowAll
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
	if p.Bridge == nil {
		return Report{}, fmt.Errorf("gitbroker: Poller.Bridge is nil")
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
		rec := p.process(ctx, ulid, filepath.Join(p.Bridge.commandsPath(), fn))
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
// end has no other way to learn what happened.
func (p *Poller) process(ctx context.Context, ulid, path string) *Receipt {
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
		return rec
	}
	var env Envelope
	if err := json.Unmarshal(raw, &env); err != nil {
		rec.Error = "unparseable envelope: " + err.Error()
		return rec
	}
	rec.Seq, rec.Agent, rec.Why = env.Seq, env.Agent, env.Why
	rec.Call = &CallEcho{Method: env.Call.Method, URL: env.Call.URL}
	if err := env.Validate(); err != nil {
		rec.Decision = Decision{Fired: false, Policy: "validate", Reason: err.Error()}
		return rec
	}
	rec.Call.Method = env.Call.Method // Validate may have defaulted it
	if env.ULID != "" && env.ULID != ulid {
		rec.Decision = Decision{Fired: false, Policy: "validate", Reason: fmt.Sprintf("envelope ulid %q does not match file name %q", env.ULID, ulid)}
		return rec
	}
	if env.Expired(p.now()) {
		rec.Decision = Decision{Fired: false, Policy: "expiry", Reason: "expired (not_after " + env.NotAfter + ")"}
		return rec
	}
	// the authoritative side decides — the far end only proposed
	d := p.policy().Decide(&env)
	rec.Decision = d
	if !d.Fired {
		return rec
	}
	call := env.Call
	if env.AuthSlot != "" {
		auth, err := p.resolveSlot(env.AuthSlot)
		if err != nil {
			rec.Decision = Decision{Fired: false, Policy: "auth-slot", Reason: err.Error()}
			return rec
		}
		auth.Apply(&call) // the credential exists only here, on this side, in memory
	}
	timeout := p.Bridge.cfg.FireTimeout
	if env.MaxMs > 0 {
		timeout = time.Duration(env.MaxMs) * time.Millisecond
	}
	fctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	out, err := p.firer().Fire(fctx, call)
	if err != nil {
		rec.Error = "fire failed: " + err.Error()
		return rec
	}
	rec.Status = out.Status
	rec.LatencyMs = out.Latency.Milliseconds()
	rec.BodyLen = len(out.Body)
	rec.BodyDigest = Digest(out.Body)
	if max := p.Bridge.cfg.MaxBody; len(out.Body) > max {
		rec.Body, rec.Truncated = string(out.Body[:max]), true
	} else {
		rec.Body = string(out.Body)
	}
	rec.Headers = map[string]string{}
	for k, v := range out.Headers {
		if len(v) > 0 {
			rec.Headers[k] = v[0]
		}
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
// file ({"<slot>": {"type":"bearer","key":"..."}, ...}). The file lives in the
// bridge checkout by default but must be git-ignored there — it is the one
// thing that never gets committed.
func (p *Poller) resolveSlot(slot string) (httpx.Auth, error) {
	b, err := os.ReadFile(p.Bridge.slotsPath())
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
