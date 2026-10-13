package gitbroker

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/rrrishi123/adapters/internal/httpx"
)

// Outcome is the afferent leg of one fired CALL, as the witness returned it.
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
	Fire(ctx context.Context, call httpx.Request) (*Outcome, error)
}

// CollectorFirer fires a CALL through the 8 collector's /fetch, which performs
// the HTTP request, records it on the ledger, and stamps the response with the
// X-8-Witness reafference. Actor is declared on every fire (X-8-Actor and the
// body's "actor") so the ledger names the far end, not "undeclared".
type CollectorFirer struct {
	Collector string       // base URL of the witness, e.g. http://127.0.0.1:7070 (config, never hardcoded)
	Actor     string       // X-8-Actor
	HC        *http.Client // nil = a 5-minute client (/fetch itself allows 4 min for device sessions)
}

// Fire implements Firer.
func (f *CollectorFirer) Fire(ctx context.Context, call httpx.Request) (*Outcome, error) {
	if strings.TrimSpace(f.Collector) == "" {
		return nil, fmt.Errorf("gitbroker: collector URL is not configured")
	}
	payload, err := json.Marshal(struct {
		httpx.Request
		Actor string `json:"actor,omitempty"`
	}{call, f.Actor})
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, strings.TrimRight(f.Collector, "/")+"/fetch", bytes.NewReader(payload))
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

// WitnessStub is a stdlib stand-in for the collector's /fetch, for loopbacks
// and tests: it performs the CALL and stamps the same X-8-Witness /
// X-8-Ledger-Id / X-8-DMs headers the real witness does. It records nothing —
// it exists so the adapter's shape can be proven with both ends owned.
func WitnessStub() http.Handler {
	var seq int64
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
		actor := in.Actor
		if actor == "" {
			actor = r.Header.Get("X-8-Actor")
		}
		if actor == "" {
			actor = "undeclared"
		}
		t0 := time.Now()
		resp, err := httpx.Do(&http.Client{Timeout: 30 * time.Second}, in.Request)
		if err != nil {
			http.Error(w, `{"error":"`+err.Error()+`"}`, http.StatusBadGateway)
			return
		}
		seq++
		d := time.Since(t0).Milliseconds()
		w.Header().Set("X-8-Witness", fmt.Sprintf("seen · act #%d · call · %dms · %s %s · by %s (stub)", seq, d, strings.ToUpper(in.Method), in.URL, actor))
		w.Header().Set("X-8-Ledger-Id", fmt.Sprint(seq))
		w.Header().Set("X-8-DMs", fmt.Sprint(d))
		if ct := http.Header(resp.Headers).Get("Content-Type"); ct != "" {
			w.Header().Set("Content-Type", ct)
		}
		w.WriteHeader(resp.Status)
		_, _ = io.WriteString(w, resp.Body)
	})
	return mux
}
