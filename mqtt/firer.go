package mqtt

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync/atomic"
	"time"

	"github.com/rrrishi123/adapters/internal/httpx"
)

// Outcome is the afferent leg of one fired atom, as the witness returned it.
type Outcome struct {
	Status  int
	Headers http.Header
	Body    []byte
	Latency time.Duration
}

// Firer fires one validated, policy-approved envelope and returns what came
// back. The production Firer is CollectorFirer (through the witness, so the
// receipt carries X-8-Witness); a test can substitute anything.
type Firer interface {
	FireCall(ctx context.Context, call httpx.Request) (*Outcome, error)
	FireChannel(ctx context.Context, op ChannelOp) (*Outcome, error)
}

// CollectorFirer fires through the 8 collector: a CALL via POST /fetch (the
// collector performs the HTTP request, records it, stamps X-8-Witness), a
// CHANNEL command via POST /run?session=<seat> (the collector sends it on the
// seat's held BiDi/CDP socket and stamps the same reafference). Actor is
// declared on every fire so the ledger names the far end, not "undeclared".
type CollectorFirer struct {
	Collector string       // base URL of the witness, e.g. http://127.0.0.1:7070 (config, never hardcoded)
	Actor     string       // X-8-Actor
	HC        *http.Client // nil = a 5-minute client (/fetch itself allows 4 min for device sessions)
}

func (f *CollectorFirer) post(ctx context.Context, path string, payload any) (*Outcome, error) {
	if strings.TrimSpace(f.Collector) == "" {
		return nil, fmt.Errorf("mqtt: collector URL is not configured (-collector / MQTT_COLLECTOR)")
	}
	raw, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, strings.TrimRight(f.Collector, "/")+path, bytes.NewReader(raw))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	if f.Actor != "" {
		req.Header.Set("X-8-Actor", f.Actor)
	}
	hc := f.HC
	if hc == nil {
		hc = &http.Client{Timeout: 5 * time.Minute}
	}
	t0 := time.Now()
	resp, err := hc.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 4<<20))
	if err != nil {
		return nil, err
	}
	return &Outcome{Status: resp.StatusCode, Headers: resp.Header, Body: body, Latency: time.Since(t0)}, nil
}

// FireCall implements Firer via POST /fetch.
func (f *CollectorFirer) FireCall(ctx context.Context, call httpx.Request) (*Outcome, error) {
	return f.post(ctx, "/fetch", struct {
		httpx.Request
		Actor string `json:"actor,omitempty"`
	}{call, f.Actor})
}

// FireChannel implements Firer via POST /run?session=<seat>.
func (f *CollectorFirer) FireChannel(ctx context.Context, op ChannelOp) (*Outcome, error) {
	return f.post(ctx, "/run?session="+url.QueryEscape(op.Session), struct {
		ID     int             `json:"id"`
		Method string          `json:"method"`
		Params json.RawMessage `json:"params"`
		Actor  string          `json:"actor,omitempty"`
	}{op.ID, op.Method, op.Params, f.Actor})
}

// WitnessStub is a stdlib stand-in for the collector, for loopbacks and
// tests: /fetch performs the CALL; /run?session=<seat> answers the CHANNEL
// command itself (the stub IS the seat). Both stamp the same X-8-Witness /
// X-8-Ledger-Id / X-8-DMs headers the real witness does. It records nothing —
// it exists so the adapter's shape can be proven with both ends owned.
func WitnessStub() http.Handler {
	var seq atomic.Int64
	stamp := func(w http.ResponseWriter, physics, what, actor string, d int64) {
		n := seq.Add(1)
		w.Header().Set("X-8-Witness", fmt.Sprintf("seen · act #%d · %s · %dms · %s · by %s (stub)", n, physics, d, what, actor))
		w.Header().Set("X-8-Ledger-Id", fmt.Sprint(n))
		w.Header().Set("X-8-DMs", fmt.Sprint(d))
	}
	actorOf := func(r *http.Request, declared string) string {
		if declared != "" {
			return declared
		}
		if a := r.Header.Get("X-8-Actor"); a != "" {
			return a
		}
		return "undeclared"
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/fetch", func(w http.ResponseWriter, r *http.Request) {
		var in struct {
			httpx.Request
			Actor string `json:"actor"`
		}
		if err := json.NewDecoder(r.Body).Decode(&in); err != nil || in.URL == "" {
			http.Error(w, `{"error":"url is required"}`, http.StatusBadRequest)
			return
		}
		if in.Method == "" {
			in.Method = "GET"
		}
		t0 := time.Now()
		resp, err := httpx.Do(&http.Client{Timeout: 30 * time.Second}, in.Request)
		if err != nil {
			http.Error(w, `{"error":"`+err.Error()+`"}`, http.StatusBadGateway)
			return
		}
		stamp(w, "call", strings.ToUpper(in.Method)+" "+in.URL, actorOf(r, in.Actor), time.Since(t0).Milliseconds())
		if ct := http.Header(resp.Headers).Get("Content-Type"); ct != "" {
			w.Header().Set("Content-Type", ct)
		}
		w.WriteHeader(resp.Status)
		_, _ = io.WriteString(w, resp.Body)
	})
	mux.HandleFunc("/run", func(w http.ResponseWriter, r *http.Request) {
		seat := r.URL.Query().Get("session")
		if seat == "" {
			http.Error(w, `{"error":"unknown or missing session — needs ?session=<id>"}`, http.StatusNotFound)
			return
		}
		var in struct {
			ID     int             `json:"id"`
			Method string          `json:"method"`
			Params json.RawMessage `json:"params"`
			Actor  string          `json:"actor"`
		}
		if err := json.NewDecoder(r.Body).Decode(&in); err != nil || in.Method == "" {
			http.Error(w, `{"error":"method is required"}`, http.StatusBadRequest)
			return
		}
		if len(in.Params) == 0 {
			in.Params = json.RawMessage(`{}`)
		}
		stamp(w, "channel", in.Method+" @"+seat, actorOf(r, in.Actor), 0)
		w.Header().Set("X-8-Ledger", fmt.Sprint(seq.Load()))
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"id":     in.ID,
			"result": map[string]any{"seat": seat, "echo": in.Method, "params": in.Params},
		})
	})
	return mux
}
