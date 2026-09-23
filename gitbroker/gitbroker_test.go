package gitbroker

// Own-both-ends loopback of the git-broker relay: a temp bridge repo (git
// init, no remote), a far end that COMMITS an envelope proposing a CALL at a
// target we host, a witness stub that performs the CALL and stamps X-8-Witness,
// and the poller that fires and commits a thick receipt back. The assertions
// are the contract: receipt exists, carries the witness line, status, digest
// and body, and is in a git commit the far end could pull.

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/rrrishi123/adapters/internal/httpx"
)

func newBridge(t *testing.T) *Bridge {
	t.Helper()
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git not on PATH")
	}
	dir := t.TempDir()
	if err := InitLocal(dir); err != nil {
		t.Fatal(err)
	}
	b, err := Open(Config{Dir: dir, PollInterval: time.Second, Actor: "far-end-test"})
	if err != nil {
		t.Fatal(err)
	}
	if b.Mode() != "local" {
		t.Fatalf("bridge mode: got %s, want local", b.Mode())
	}
	return b
}

// commitEnvelope plays the far end: write commands/<ulid>.json and commit it.
func commitEnvelope(t *testing.T, b *Bridge, env Envelope) {
	t.Helper()
	path := filepath.Join(b.commandsPath(), env.ULID+".json")
	if err := writeJSONAtomic(path, env); err != nil {
		t.Fatal(err)
	}
	for _, args := range [][]string{
		{"add", "--", b.cfg.CommandsDir},
		{"-c", "user.name=far-end", "-c", "user.email=far@end", "-c", "commit.gpgsign=false", "commit", "-q", "-m", "propose: " + env.ULID},
	} {
		if out, err := gitRun(b.cfg.Dir, args...); err != nil {
			t.Fatalf("far end git %v: %v %s", args, err, out)
		}
	}
}

func TestGitbroker_Loopback_CALL_roundTrip(t *testing.T) {
	// the target: what the far end wants reached
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w, `{"echo":%q,"method":%q}`, r.URL.Query().Get("q"), r.Method)
	}))
	defer target.Close()
	// the witness: performs the CALL and stamps the reafference
	witness := httptest.NewServer(WitnessStub())
	defer witness.Close()

	b := newBridge(t)
	b.cfg.Collector = witness.URL
	commitEnvelope(t, b, Envelope{
		Schema: EnvelopeSchema, ULID: "01LOOPBACK0000000000000001", Seq: 1, Agent: "far-end",
		Atom: AtomCall,
		Call: httpx.Request{Method: "GET", URL: target.URL + "/ping?q=hello"},
		Why:  "loopback: does a proposal come back as a witnessed receipt?",
	})

	p := &Poller{Bridge: b, Policy: AllowAll} // explicit: nil is Closed
	rep, err := p.Once(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if rep.Fired != 1 || len(rep.Processed) != 1 || rep.Commit == "" {
		t.Fatalf("report: %+v", rep)
	}

	// the far end's read side
	f, err := os.Open(filepath.Join(b.receiptsPath(), "01LOOPBACK0000000000000001.json"))
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	rec, err := ReadReceipt(f)
	if err != nil {
		t.Fatal(err)
	}
	if rec.Schema != ReceiptSchema || rec.Atom != AtomCall || !rec.Decision.Fired {
		t.Fatalf("receipt shape: %+v", rec)
	}
	if rec.Status != 200 || !strings.Contains(rec.Body, `"echo":"hello"`) {
		t.Fatalf("afferent leg not carried: status=%d body=%q", rec.Status, rec.Body)
	}
	if !strings.HasPrefix(rec.Witness.Line, "seen · act #") || rec.Witness.LedgerID == "" {
		t.Fatalf("receipt lacks the X-8-Witness reafference: %+v", rec.Witness)
	}
	if rec.BodyDigest != Digest([]byte(rec.Body)) || rec.Truncated {
		t.Fatalf("digest/body mismatch: %+v", rec)
	}
	if rec.Call == nil || rec.Call.URL != target.URL+"/ping?q=hello" {
		t.Fatalf("call echo: %+v", rec.Call)
	}
	if rec.Headers["Content-Type"] != "application/json" || rec.Headers["X-8-Witness"] == "" {
		t.Fatalf("allowlisted headers must still be carried: %v", rec.Headers)
	}
	// the receipt is IN GIT — the far end learns by pulling
	out, err := gitRun(b.cfg.Dir, "log", "--oneline", "-1", "--", b.cfg.ReceiptsDir)
	if err != nil || !strings.Contains(out, "receipts: 01LOOPBACK0000000000000001") {
		t.Fatalf("receipt not committed: %v %q", err, out)
	}
	if st, _ := gitRun(b.cfg.Dir, "status", "--porcelain"); strings.TrimSpace(st) != "" {
		t.Fatalf("bridge dirty after poll: %q", st)
	}

	// second poll: idempotent — nothing re-fires, nothing re-commits
	rep2, err := p.Once(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(rep2.Processed) != 0 || rep2.Commit != "" {
		t.Fatalf("not idempotent: %+v", rep2)
	}
	t.Logf("receipt witness: %s", rec.Witness.Line)
	t.Logf("receipt commit:  %s", strings.TrimSpace(out))
}

func TestGitbroker_FarEndOnlyProposes_policyRefuses(t *testing.T) {
	b := newBridge(t)
	fired := 0
	p := &Poller{
		Bridge: b,
		Firer: fireFunc(func(context.Context, httpx.Request) (*Outcome, error) {
			fired++
			return &Outcome{Status: 200}, nil
		}),
		Policy: policyFile(t, `{"allow_url_prefixes":["http://allowed.local/"]}`),
	}

	commitEnvelope(t, b, Envelope{ULID: "a-allowed", Seq: 1, Call: httpx.Request{URL: "http://allowed.local/x"}})
	commitEnvelope(t, b, Envelope{ULID: "b-denied", Seq: 2, Call: httpx.Request{URL: "http://elsewhere.local/x"}})
	commitEnvelope(t, b, Envelope{ULID: "c-channel", Seq: 3, Atom: "channel", Call: httpx.Request{URL: "http://allowed.local/x"}})
	commitEnvelope(t, b, Envelope{ULID: "d-expired", Seq: 4, NotAfter: "2020-01-01T00:00:00Z", Call: httpx.Request{URL: "http://allowed.local/x"}})
	commitEnvelope(t, b, Envelope{ULID: "e-secret", Seq: 5, Call: httpx.Request{URL: "http://allowed.local/x", Headers: map[string]string{"Authorization": "Bearer leaked"}}})

	rep, err := p.Once(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if fired != 1 || rep.Fired != 1 || rep.Refused != 4 {
		t.Fatalf("policy: fired=%d report=%+v", fired, rep)
	}
	want := map[string]string{
		"b-denied":  "file-policy",
		"c-channel": "validate",
		"d-expired": "expiry",
		"e-secret":  "validate",
	}
	for ulid, policy := range want {
		f, err := os.Open(filepath.Join(b.receiptsPath(), ulid+".json"))
		if err != nil {
			t.Fatal(err)
		}
		rec, err := ReadReceipt(f)
		f.Close()
		if err != nil {
			t.Fatal(err)
		}
		if rec.Decision.Fired || rec.Decision.Policy != policy || rec.Decision.Reason == "" {
			t.Fatalf("%s: want refused by %s, got %+v", ulid, policy, rec.Decision)
		}
	}
}

// TestGitbroker_SchemasMatchTypes keeps envelope.schema.json / receipt.schema.json
// honest: every "required" key in the schema must appear in what the Go types
// emit, and every emitted key must be declared in the schema's properties.
func TestGitbroker_SchemasMatchTypes(t *testing.T) {
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
		json.Unmarshal(b, &got)
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
		Call:     httpx.Request{Method: "GET", URL: "http://x/", Headers: map[string]string{"A": "b"}, Body: "{}"},
		AuthSlot: "s", NotAfter: "2030-01-01T00:00:00Z", MaxMs: 1, Why: "w", ProposedAt: "2030-01-01T00:00:00Z"})
	check("receipt.schema.json", Receipt{Schema: ReceiptSchema, Contract: "v", ULID: "x", Seq: 1, Agent: "a", Atom: AtomCall, Why: "w",
		Decision: Decision{Fired: true, Policy: "p", Reason: "r", Egress: &Egress{Body: true, Headers: true, AuthenticatedBody: true}}, Call: &CallEcho{Method: "GET", URL: "http://x/"},
		Status: 200, LatencyMs: 1, BodyLen: 1, BodyDigest: "d", Body: "b", Truncated: true, Headers: map[string]string{"A": "b"}, Authenticated: true,
		Witness: Witness{Line: "l", LedgerID: "1", Ledger: "2", DMs: "3"}, Error: "e", ProcessedAt: "t", Poller: PollerID{Actor: "a", Host: "h"}})
}

type fireFunc func(context.Context, httpx.Request) (*Outcome, error)

func (f fireFunc) Fire(ctx context.Context, c httpx.Request) (*Outcome, error) { return f(ctx, c) }

// policyFile writes a policy OUTSIDE any bridge (its own temp dir) and returns
// the FilePolicy — the shape every real deployment must use.
func policyFile(t *testing.T, doc string) FilePolicy {
	t.Helper()
	pf := filepath.Join(t.TempDir(), "policy.json")
	if err := os.WriteFile(pf, []byte(doc), 0o644); err != nil {
		t.Fatal(err)
	}
	return FilePolicy{Path: pf}
}

func readReceipt(t *testing.T, b *Bridge, ulid string) *Receipt {
	t.Helper()
	f, err := os.Open(filepath.Join(b.receiptsPath(), ulid+".json"))
	if err != nil {
		t.Fatalf("no receipt for %s: %v", ulid, err)
	}
	defer f.Close()
	rec, err := ReadReceipt(f)
	if err != nil {
		t.Fatal(err)
	}
	return rec
}

// --- the relay security gate (#1145) ---

// G1: the policy must live outside the bridge checkout; a poller configured
// with one inside REFUSES TO RUN (no receipt, no fire), symlinks included.
func TestGitbroker_G1_PolicyInsideBridgeRefusesToStart(t *testing.T) {
	b := newBridge(t)
	inside := filepath.Join(b.cfg.Dir, "state", "policy.json")
	os.MkdirAll(filepath.Dir(inside), 0o755)
	os.WriteFile(inside, []byte(`{}`), 0o644)
	if _, err := NewFilePolicy(inside, b.cfg.Dir); err == nil {
		t.Fatal("NewFilePolicy must refuse a path inside the bridge")
	}
	if _, err := NewFilePolicy(filepath.Join(b.cfg.Dir, "..", filepath.Base(b.cfg.Dir), "state", "policy.json"), b.cfg.Dir); err == nil {
		t.Fatal("a dotted path back into the bridge must be refused")
	}
	fired := 0
	p := &Poller{Bridge: b, Policy: FilePolicy{Path: inside}, Firer: fireFunc(func(context.Context, httpx.Request) (*Outcome, error) {
		fired++
		return &Outcome{Status: 200}, nil
	})}
	commitEnvelope(t, b, Envelope{ULID: "g1", Seq: 1, Call: httpx.Request{URL: "http://allowed.local/x"}})
	if _, err := p.Once(context.Background()); err == nil || !strings.Contains(err.Error(), "inside the public working tree") {
		t.Fatalf("Once must refuse to run with the policy inside the bridge, got %v", err)
	}
	if err := p.Run(context.Background()); err == nil {
		t.Fatal("Run must refuse too")
	}
	if fired != 0 {
		t.Fatal("nothing may fire")
	}
	if _, err := os.Stat(filepath.Join(b.receiptsPath(), "g1.json")); err == nil {
		t.Fatal("no receipt may be written by a poller that refused to start")
	}
	// outside: runs
	p.Policy = policyFile(t, `{}`)
	rep, err := p.Once(context.Background())
	if err != nil || rep.Fired != 1 {
		t.Fatalf("policy outside must run: %v %+v", err, rep)
	}
}

// G1: a MISSING policy file is CLOSED, and so is a poller with no Policy at all.
func TestGitbroker_G1_MissingPolicyIsClosed(t *testing.T) {
	b := newBridge(t)
	fired := 0
	firer := fireFunc(func(context.Context, httpx.Request) (*Outcome, error) {
		fired++
		return &Outcome{Status: 200}, nil
	})
	missing := filepath.Join(t.TempDir(), "does-not-exist.json")
	p := &Poller{Bridge: b, Firer: firer, Policy: FilePolicy{Path: missing}}
	commitEnvelope(t, b, Envelope{ULID: "m1", Seq: 1, Call: httpx.Request{URL: "http://allowed.local/x"}})
	rep, err := p.Once(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if fired != 0 || rep.Fired != 0 || rep.Refused != 1 {
		t.Fatalf("missing policy must be closed: fired=%d %+v", fired, rep)
	}
	rec := readReceipt(t, b, "m1")
	if rec.Decision.Fired || !strings.Contains(rec.Decision.Reason, "missing") || !strings.Contains(rec.Decision.Reason, "closed") {
		t.Fatalf("receipt must say missing = closed: %+v", rec.Decision)
	}
	// nil Policy: closed, not allow-all
	p2 := &Poller{Bridge: b, Firer: firer}
	commitEnvelope(t, b, Envelope{ULID: "m2", Seq: 2, Call: httpx.Request{URL: "http://allowed.local/x"}})
	if _, err := p2.Once(context.Background()); err != nil {
		t.Fatal(err)
	}
	if rec := readReceipt(t, b, "m2"); rec.Decision.Fired || rec.Decision.Policy != "closed" {
		t.Fatalf("nil policy must be closed: %+v", rec.Decision)
	}
	if fired != 0 {
		t.Fatal("nothing may fire without a policy")
	}
}

// G2: the slots file must live outside the bridge; Open refuses one inside,
// and a poller with no slots file refuses every auth_slot envelope.
func TestGitbroker_G2_SlotsOutsideBridge(t *testing.T) {
	dir := t.TempDir()
	if err := InitLocal(dir); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(Config{Dir: dir, Slots: filepath.Join(dir, "state", "slots.json")}); err == nil || !strings.Contains(err.Error(), "inside the public working tree") {
		t.Fatalf("Open must refuse a slots file inside the bridge, got %v", err)
	}
	outside := filepath.Join(t.TempDir(), "slots.json")
	os.WriteFile(outside, []byte(`{"lab":{"type":"bearer","key":"never-committed"}}`), 0o600)
	b, err := Open(Config{Dir: dir, Slots: outside, Actor: "t"})
	if err != nil {
		t.Fatal(err)
	}
	if !filepath.IsAbs(b.Config().Slots) {
		t.Fatalf("slots path must be absolute: %q", b.Config().Slots)
	}
	// no slots configured → auth_slot refused, never fired
	b2 := newBridge(t)
	fired := 0
	p := &Poller{Bridge: b2, Policy: AllowAll, Firer: fireFunc(func(context.Context, httpx.Request) (*Outcome, error) {
		fired++
		return &Outcome{Status: 200}, nil
	})}
	commitEnvelope(t, b2, Envelope{ULID: "s1", Seq: 1, Call: httpx.Request{URL: "http://x.local/"}, AuthSlot: "lab"})
	if _, err := p.Once(context.Background()); err != nil {
		t.Fatal(err)
	}
	if rec := readReceipt(t, b2, "s1"); rec.Decision.Fired || rec.Decision.Policy != "auth-slot" || fired != 0 {
		t.Fatalf("auth_slot without a slots file must refuse: %+v fired=%d", rec.Decision, fired)
	}
}

// G3: response headers are allowlisted — a Set-Cookie (or any server header)
// is NOT in the receipt; Content-Type / Content-Length / X-8-* are. And a
// slot-authenticated call publishes only the body digest unless the policy
// opts in with egress.authenticated_body.
func TestGitbroker_G3_ReceiptEgress(t *testing.T) {
	dir := t.TempDir()
	if err := InitLocal(dir); err != nil {
		t.Fatal(err)
	}
	slots := filepath.Join(t.TempDir(), "slots.json")
	os.WriteFile(slots, []byte(`{"lab":{"type":"bearer","key":"never-committed"}}`), 0o600)
	b, err := Open(Config{Dir: dir, Slots: slots, Actor: "t", MaxBody: 8192})
	if err != nil {
		t.Fatal(err)
	}
	var sawAuth string
	firer := fireFunc(func(_ context.Context, c httpx.Request) (*Outcome, error) {
		sawAuth = c.Headers["Authorization"]
		h := http.Header{}
		h.Set("Content-Type", "application/json")
		h.Set("Content-Length", "17")
		h.Set("X-8-Witness", "seen · act #9 · call")
		h.Set("X-8-Ledger-Id", "9")
		h.Add("Set-Cookie", "session=TOPSECRET; HttpOnly")
		h.Set("WWW-Authenticate", "Bearer realm=private")
		h.Set("Server", "nginx/1.0")
		return &Outcome{Status: 200, Headers: h, Body: []byte(`{"private":true}`)}, nil
	})
	p := &Poller{Bridge: b, Firer: firer, Policy: policyFile(t, `{}`)}
	commitEnvelope(t, b, Envelope{ULID: "plain", Seq: 1, Call: httpx.Request{URL: "http://x.local/"}})
	commitEnvelope(t, b, Envelope{ULID: "slot", Seq: 2, Call: httpx.Request{URL: "http://x.local/"}, AuthSlot: "lab"})
	if _, err := p.Once(context.Background()); err != nil {
		t.Fatal(err)
	}
	if sawAuth != "Bearer never-committed" {
		t.Fatalf("slot must be applied host-side on the fire: %q", sawAuth)
	}
	for _, ulid := range []string{"plain", "slot"} {
		raw, err := os.ReadFile(filepath.Join(b.receiptsPath(), ulid+".json"))
		if err != nil {
			t.Fatal(err)
		}
		for _, leak := range []string{"Set-Cookie", "TOPSECRET", "WWW-Authenticate", "nginx", "never-committed"} {
			if strings.Contains(string(raw), leak) {
				t.Fatalf("%s: %q leaked into the committed receipt:\n%s", ulid, leak, raw)
			}
		}
		rec := readReceipt(t, b, ulid)
		if !rec.Decision.Fired || rec.Status != 200 {
			t.Fatalf("%s: %+v", ulid, rec.Decision)
		}
		if rec.Headers["Content-Type"] != "application/json" || rec.Headers["Content-Length"] != "17" || rec.Headers["X-8-Witness"] == "" || rec.Headers["X-8-Ledger-Id"] != "9" || len(rec.Headers) != 4 {
			t.Fatalf("%s: allowlisted headers wrong: %v", ulid, rec.Headers)
		}
		if rec.BodyDigest != Digest([]byte(`{"private":true}`)) || rec.BodyLen != 16 {
			t.Fatalf("%s: digest/len must always be present: %+v", ulid, rec)
		}
	}
	if rec := readReceipt(t, b, "plain"); rec.Body != `{"private":true}` || rec.Authenticated {
		t.Fatalf("plain call publishes its body: %+v", rec)
	}
	if rec := readReceipt(t, b, "slot"); rec.Body != "" || !rec.Authenticated {
		t.Fatalf("slot-authenticated call must publish digest only by default: body=%q auth=%v", rec.Body, rec.Authenticated)
	}
	// explicit opt-in publishes the authenticated body
	p.Policy = policyFile(t, `{"egress":{"body":true,"headers":true,"authenticated_body":true}}`)
	commitEnvelope(t, b, Envelope{ULID: "slot-optin", Seq: 3, Call: httpx.Request{URL: "http://x.local/"}, AuthSlot: "lab"})
	if _, err := p.Once(context.Background()); err != nil {
		t.Fatal(err)
	}
	if rec := readReceipt(t, b, "slot-optin"); rec.Body != `{"private":true}` || rec.Decision.Egress == nil || !rec.Decision.Egress.AuthenticatedBody {
		t.Fatalf("opt-in must publish: %+v", rec)
	}
	// egress off entirely: no body, no headers, digest stays
	p.Policy = policyFile(t, `{"egress":{"body":false,"headers":false}}`)
	commitEnvelope(t, b, Envelope{ULID: "dark", Seq: 4, Call: httpx.Request{URL: "http://x.local/"}})
	if _, err := p.Once(context.Background()); err != nil {
		t.Fatal(err)
	}
	if rec := readReceipt(t, b, "dark"); rec.Body != "" || rec.Headers != nil || rec.BodyDigest == "" || rec.Witness.Line == "" {
		t.Fatalf("dark egress: %+v", rec)
	}
}

// G4: request headers are an allowlist and the URL is checked structurally —
// userinfo, ?access_key=, X-Api-Key, X-Auth-Token, Cookie all refuse at
// Validate, before policy, before any fire.
func TestGitbroker_G4_CredentialShapesRefused(t *testing.T) {
	b := newBridge(t)
	fired := 0
	p := &Poller{Bridge: b, Policy: AllowAll, Firer: fireFunc(func(context.Context, httpx.Request) (*Outcome, error) {
		fired++
		return &Outcome{Status: 200}, nil
	})}
	bad := map[string]httpx.Request{
		"userinfo":     {URL: "https://user:key@host.local/path"},
		"userinfo-tok": {URL: "https://tok@host.local/path"},
		"access-key":   {URL: "http://host.local/api?access_key=abc"},
		"api-key-hdr":  {URL: "http://host.local/", Headers: map[string]string{"X-Api-Key": "k"}},
		"auth-token":   {URL: "http://host.local/", Headers: map[string]string{"X-Auth-Token": "k"}},
		"cookie":       {URL: "http://host.local/", Headers: map[string]string{"Cookie": "sid=1"}},
		"authz":        {URL: "http://host.local/", Headers: map[string]string{"Authorization": "Bearer x"}},
		"unknown-hdr":  {URL: "http://host.local/", Headers: map[string]string{"X-Anything-Else": "v"}},
		"file-scheme":  {URL: "file:///etc/passwd"},
	}
	seq := int64(0)
	for ulid, call := range bad {
		seq++
		commitEnvelope(t, b, Envelope{ULID: ulid, Seq: seq, Call: call})
	}
	commitEnvelope(t, b, Envelope{ULID: "zz-ok", Seq: seq + 1, Call: httpx.Request{URL: "http://host.local/?q=1", Headers: map[string]string{"Accept": "*/*", "X-8-Actor": "far"}}})
	rep, err := p.Once(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if fired != 1 || rep.Fired != 1 || rep.Refused != len(bad) {
		t.Fatalf("fired=%d %+v", fired, rep)
	}
	for ulid := range bad {
		rec := readReceipt(t, b, ulid)
		if rec.Decision.Fired || rec.Decision.Policy != "validate" || rec.Decision.Reason == "" {
			t.Fatalf("%s: must be refused at validate: %+v", ulid, rec.Decision)
		}
		if strings.Contains(rec.Decision.Reason, "key@") || strings.Contains(rec.Decision.Reason, "Bearer x") {
			t.Fatalf("%s: the refusal must not echo the credential: %q", ulid, rec.Decision.Reason)
		}
	}
	if rec := readReceipt(t, b, "zz-ok"); !rec.Decision.Fired {
		t.Fatalf("allowlisted headers must pass: %+v", rec.Decision)
	}
}

// G5: HALT — the far end's revocation (state/halt in the bridge) is checked
// before every fire; a halted poll answers nothing and the envelopes fire once
// the halt is lifted.
func TestGitbroker_G5_HaltBeforeEveryFire(t *testing.T) {
	b := newBridge(t)
	halt := filepath.Join(b.cfg.Dir, "state", "halt")
	fired := 0
	p := &Poller{Bridge: b, Policy: AllowAll, Firer: fireFunc(func(context.Context, httpx.Request) (*Outcome, error) {
		fired++
		if fired == 1 { // the far end revokes mid-poll, after the first fire
			os.MkdirAll(filepath.Dir(halt), 0o755)
			os.WriteFile(halt, nil, 0o644)
		}
		return &Outcome{Status: 200}, nil
	})}
	commitEnvelope(t, b, Envelope{ULID: "h1", Seq: 1, Call: httpx.Request{URL: "http://x.local/1"}})
	commitEnvelope(t, b, Envelope{ULID: "h2", Seq: 2, Call: httpx.Request{URL: "http://x.local/2"}})
	commitEnvelope(t, b, Envelope{ULID: "h3", Seq: 3, Call: httpx.Request{URL: "http://x.local/3"}})
	rep, err := p.Once(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if fired != 1 || rep.Fired != 1 || !rep.Halted || len(rep.Processed) != 1 {
		t.Fatalf("halt after the first fire must stop the poll: fired=%d %+v", fired, rep)
	}
	for _, u := range []string{"h2", "h3"} {
		if _, err := os.Stat(filepath.Join(b.receiptsPath(), u+".json")); err == nil {
			t.Fatalf("%s must stay unanswered while halted", u)
		}
	}
	// still halted: nothing fires at all
	rep, err = p.Once(context.Background())
	if err != nil || fired != 1 || !rep.Halted || len(rep.Processed) != 0 {
		t.Fatalf("still halted: %v fired=%d %+v", err, fired, rep)
	}
	// lifted: the rest fire
	os.Remove(halt)
	rep, err = p.Once(context.Background())
	if err != nil || fired != 3 || rep.Halted || rep.Fired != 2 {
		t.Fatalf("after halt lifted: %v fired=%d %+v", err, fired, rep)
	}
}
