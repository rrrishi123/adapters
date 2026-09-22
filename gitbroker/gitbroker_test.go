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

	p := &Poller{Bridge: b}
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
		Policy: FilePolicy{Path: filepath.Join(b.cfg.Dir, "state", "policy.json")},
	}
	os.MkdirAll(filepath.Join(b.cfg.Dir, "state"), 0o755)
	os.WriteFile(filepath.Join(b.cfg.Dir, "state", "policy.json"), []byte(`{"allow_url_prefixes":["http://allowed.local/"]}`), 0o644)

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
		Decision: Decision{Fired: true, Policy: "p", Reason: "r"}, Call: &CallEcho{Method: "GET", URL: "http://x/"},
		Status: 200, LatencyMs: 1, BodyLen: 1, BodyDigest: "d", Body: "b", Truncated: true, Headers: map[string]string{"A": "b"},
		Witness: Witness{Line: "l", LedgerID: "1", Ledger: "2", DMs: "3"}, Error: "e", ProcessedAt: "t", Poller: PollerID{Actor: "a", Host: "h"}})
}

type fireFunc func(context.Context, httpx.Request) (*Outcome, error)

func (f fireFunc) Fire(ctx context.Context, c httpx.Request) (*Outcome, error) { return f(ctx, c) }
