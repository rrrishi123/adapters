// loopback — fire a REAL own-both-ends round-trip for the transports the wire
// delegates to adapters, as a runnable command (not only a test). This is what
// makes the cockpit's WIRE pane rows for grpc/mqtt/webrtc/unix genuinely
// fireable: the 8 collector execs `loopback <transport>` and witnesses the
// result into the feed. Same harnesses the conformance tests prove, packaged
// for runtime: we are BOTH ends (WebRTC uses Pion for ICE/DTLS/SCTP).
//
//	loopback grpc    → unary=CALL + server-stream=CHANNEL over real HTTP/2
//	loopback mqtt    → the persistent broker relay: a far end and a host both
//	                   dial OUT to an MQTT 3.1.1 broker we host in-process; a
//	                   CALL envelope (QoS 1) is fired through a witness stub and
//	                   answered by a retained receipt (X-8-Witness inside)
//	loopback webrtc  → real DataChannel: one httpx signaling CALL, then three
//	                   CHANNEL commands + a peer event; localhost, no STUN
//	loopback unix    → the same CALL bytes over a unix-domain socket (no TCP)
//	loopback gitbroker → an envelope committed into a temp bridge repo is polled,
//	                   fired as one CALL through a witness stub, and a thick
//	                   receipt (X-8-Witness inside) is committed back
//
// Output: one JSON line {transport, ok, ms, detail} on stdout; exit 1 on fail.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"github.com/rrrishi123/adapters/gitbroker"
	"github.com/rrrishi123/adapters/grpc"
	"github.com/rrrishi123/adapters/internal/httpx"
	"github.com/rrrishi123/adapters/mqtt"
)

type result struct {
	Transport string `json:"transport"`
	OK        bool   `json:"ok"`
	Ms        int64  `json:"ms"`
	Detail    string `json:"detail"`
}

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "usage: loopback <grpc|mqtt|webrtc|unix|gitbroker>")
		os.Exit(2)
	}
	t0 := time.Now()
	var detail string
	var err error
	switch os.Args[1] {
	case "grpc":
		detail, err = fireGRPC()
	case "mqtt":
		detail, err = fireMQTT()
	case "webrtc":
		detail, err = fireWebRTC()
	case "unix":
		detail, err = fireUnix()
	case "gitbroker":
		detail, err = fireGitbroker()
	default:
		fmt.Fprintln(os.Stderr, "unknown transport "+os.Args[1])
		os.Exit(2)
	}
	r := result{Transport: os.Args[1], OK: err == nil, Ms: time.Since(t0).Milliseconds(), Detail: detail}
	if err != nil {
		r.Detail = err.Error()
	}
	json.NewEncoder(os.Stdout).Encode(r)
	if err != nil {
		os.Exit(1)
	}
}

// fireGRPC — the adapter's own service on real HTTP/2 (ALPN h2), driven through
// gRPC framing: one unary CALL, one server-stream CHANNEL.
func fireGRPC() (string, error) {
	srv := httptest.NewUnstartedServer(grpc.Handler())
	srv.EnableHTTP2 = true
	srv.StartTLS()
	defer srv.Close()
	c := &grpc.Client{HC: srv.Client(), Base: srv.URL}
	echo, err := c.Unary("wire")
	if err != nil {
		return "", fmt.Errorf("unary: %w", err)
	}
	frames, err := c.Stream("tick")
	if err != nil {
		return "", fmt.Errorf("stream: %w", err)
	}
	return fmt.Sprintf("unary CALL %q · server-stream CHANNEL %d frames over h2", echo, len(frames)), nil
}

// fireMQTT — the persistent broker relay with both ends owned: an in-process
// MQTT 3.1.1 broker, a host node that holds a persistent subscription and
// fires through a witness stub, and a far-end node that proposes one CALL
// envelope and consumes the retained receipt. Both nodes only dial OUT.
func fireMQTT() (string, error) {
	br, err := mqtt.NewBroker()
	if err != nil {
		return "", err
	}
	defer br.Close()
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, `{"transport":"mqtt","ok":true}`)
	}))
	defer target.Close()
	witness := httptest.NewServer(mqtt.WitnessStub())
	defer witness.Close()
	cfg := mqtt.Config{Broker: br.URL(), Prefix: "wire-loopback", Collector: witness.URL, KeepAlive: 5 * time.Second}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	hcfg := cfg
	hcfg.Node, hcfg.Actor = "loopback-host", "loopback-host"
	host := &mqtt.Host{Config: hcfg, Policy: mqtt.AllowAll} // explicit: a nil Policy is Closed (#1145)
	hctx, hstop := context.WithCancel(ctx)
	hdone := make(chan error, 1)
	go func() { hdone <- host.Serve(hctx) }()
	defer func() { hstop(); <-hdone }()
	select {
	case <-host.Ready():
	case err := <-hdone:
		return "", fmt.Errorf("host: %w", err)
	}

	fcfg := cfg
	fcfg.Node = "loopback-far-end"
	env := &mqtt.Envelope{ULID: "loopback-1", Seq: 1, Atom: mqtt.AtomCall, Call: &httpx.Request{Method: "GET", URL: target.URL + "/ping"}, Why: "loopback"}
	rec, err := (&mqtt.FarEnd{Config: fcfg}).Fire(ctx, env)
	if err != nil {
		return "", err
	}
	if !rec.Decision.Fired || rec.Status != 200 || rec.Witness.Line == "" || !rec.Route.Retain {
		return "", fmt.Errorf("thin or missing receipt: fired=%v status=%d witness=%q retain=%v", rec.Decision.Fired, rec.Status, rec.Witness.Line, rec.Route.Retain)
	}
	if _, ok := br.Retained(rec.Route.Topic); !ok {
		return "", fmt.Errorf("receipt not retained at the broker on %s", rec.Route.Topic)
	}
	return fmt.Sprintf("envelope → commands/%s (QoS 1) → host fired via witness → retained receipt on %s (%s)", env.ULID, rec.Route.Topic, rec.Witness.Line), nil
}

// fireWebRTC runs the separately built transport primitive beside this binary.
// The process boundary keeps Pion out of the stdlib-only parent module.
func fireWebRTC() (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	executable, err := os.Executable()
	if err != nil {
		return "", err
	}
	out, err := exec.CommandContext(ctx, filepath.Join(filepath.Dir(executable), "webrtc"), "loopback").CombinedOutput()
	if err != nil {
		return "", fmt.Errorf("webrtc loopback (build both binaries with ./build.sh): %w: %s", err, strings.TrimSpace(string(out)))
	}
	return strings.TrimSpace(string(out)), nil
}

// fireUnix — the same CALL bytes over a unix-domain socket: no TCP port exists.
func fireUnix() (string, error) {
	dir, err := os.MkdirTemp("/tmp", "w8")
	if err != nil {
		return "", err
	}
	defer os.RemoveAll(dir)
	sock := filepath.Join(dir, "wire.sock")
	ln, err := net.Listen("unix", sock)
	if err != nil {
		return "", err
	}
	defer ln.Close()
	srv := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, `{"transport":"unix","ok":true}`)
	})}
	go srv.Serve(ln)
	defer srv.Close()
	cl := &http.Client{Transport: &http.Transport{DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
		return net.Dial("unix", sock)
	}}, Timeout: 3 * time.Second}
	resp, err := cl.Get("http://unix/ping")
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != 200 || !strings.Contains(string(body), `"ok":true`) {
		return "", fmt.Errorf("bad reply %d %s", resp.StatusCode, body)
	}
	return "CALL over a unix-domain socket round-tripped — no TCP port involved", nil
}

// fireGitbroker — the store-and-forward relay with both ends owned: a temp
// bridge repo (git init, no remote), a far end that commits one envelope
// proposing a CALL at a target we host, a witness stub that performs it and
// stamps X-8-Witness, and the poller that commits the thick receipt back.
func fireGitbroker() (string, error) {
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, `{"transport":"gitbroker","ok":true}`)
	}))
	defer target.Close()
	witness := httptest.NewServer(gitbroker.WitnessStub())
	defer witness.Close()
	dir, err := os.MkdirTemp("", "bridge-loopback")
	if err != nil {
		return "", err
	}
	defer os.RemoveAll(dir)
	if err := gitbroker.InitLocal(dir); err != nil {
		return "", err
	}
	b, err := gitbroker.Open(gitbroker.Config{Dir: dir, Collector: witness.URL, Actor: "loopback-far-end", PollInterval: time.Second})
	if err != nil {
		return "", err
	}
	env := gitbroker.Envelope{Schema: gitbroker.EnvelopeSchema, ULID: "loopback-1", Seq: 1, Agent: "loopback-far-end", Atom: gitbroker.AtomCall,
		Call: httpx.Request{Method: "GET", URL: target.URL + "/ping"}, Why: "loopback"}
	raw, _ := json.Marshal(env)
	if err := os.WriteFile(filepath.Join(dir, "commands", env.ULID+".json"), raw, 0o644); err != nil {
		return "", err
	}
	rep, err := (&gitbroker.Poller{Bridge: b, Policy: gitbroker.AllowAll}).Once(context.Background()) // explicit: a nil Policy is Closed (#1145)
	if err != nil {
		return "", err
	}
	f, err := os.Open(filepath.Join(dir, "receipts", env.ULID+".json"))
	if err != nil {
		return "", fmt.Errorf("no receipt: %w", err)
	}
	defer f.Close()
	rec, err := gitbroker.ReadReceipt(f)
	if err != nil {
		return "", err
	}
	if !rec.Decision.Fired || rec.Status != 200 || rec.Witness.Line == "" || rep.Commit == "" {
		return "", fmt.Errorf("thin or missing receipt: fired=%v status=%d witness=%q commit=%q", rec.Decision.Fired, rec.Status, rec.Witness.Line, rep.Commit)
	}
	return fmt.Sprintf("envelope → poll → CALL fired via witness → receipt committed %s (%s)", rep.Commit, rec.Witness.Line), nil
}
