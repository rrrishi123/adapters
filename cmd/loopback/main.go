// loopback — fire a REAL own-both-ends round-trip for the transports the wire
// delegates to adapters, as a runnable command (not only a test). This is what
// makes the cockpit's WIRE pane rows for grpc/mqtt/webrtc/unix genuinely
// fireable: the 8 collector execs `loopback <transport>` and witnesses the
// result into the feed. Same harnesses the conformance tests prove, packaged
// for runtime: we are BOTH ends, stdlib only.
//
//	loopback grpc    → unary=CALL + server-stream=CHANNEL over real HTTP/2
//	loopback mqtt    → publish=CALL reaches subscribe=CHANNEL via a real
//	                   MQTT 3.1.1 broker we host in-process
//	loopback webrtc  → SDP offer→answer over a real signaling CALL (the only
//	                   wire-visible surface; DTLS/SCTP stays adapter-interior)
//	loopback unix    → the same CALL bytes over a unix-domain socket (no TCP)
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
	"path/filepath"
	"strings"
	"time"

	"github.com/rrrishi123/adapters/grpc"
	"github.com/rrrishi123/adapters/mqtt"
	"github.com/rrrishi123/adapters/webrtc"
)

type result struct {
	Transport string `json:"transport"`
	OK        bool   `json:"ok"`
	Ms        int64  `json:"ms"`
	Detail    string `json:"detail"`
}

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "usage: loopback <grpc|mqtt|webrtc|unix>")
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

// fireMQTT — in-process MQTT 3.1.1 broker; a publish (CALL) must arrive on a
// held subscription (CHANNEL).
func fireMQTT() (string, error) {
	br, err := mqtt.NewBroker()
	if err != nil {
		return "", err
	}
	defer br.Close()
	sub, err := mqtt.Dial(br.Addr(), "sub")
	if err != nil {
		return "", fmt.Errorf("subscriber: %w", err)
	}
	defer sub.Close()
	ch, err := sub.Subscribe("wire/loopback")
	if err != nil {
		return "", fmt.Errorf("subscribe: %w", err)
	}
	pub, err := mqtt.Dial(br.Addr(), "pub")
	if err != nil {
		return "", fmt.Errorf("publisher: %w", err)
	}
	defer pub.Close()
	if err := pub.Publish("wire/loopback", []byte("fired-by-8")); err != nil {
		return "", fmt.Errorf("publish: %w", err)
	}
	select {
	case m := <-ch:
		return fmt.Sprintf("publish CALL → held CHANNEL delivered %q via our own broker", m), nil
	case <-time.After(3 * time.Second):
		return "", fmt.Errorf("message never arrived on the channel")
	}
}

// fireWebRTC — the signaling CALL (the wire's only WebRTC surface): POST a real
// SDP offer, get a coherent answer (role flipped, own fingerprint).
func fireWebRTC() (string, error) {
	const fp = "AA:BB:CC:DD:EE:FF:00:11:22:33:44:55:66:77:88:99"
	srv := httptest.NewServer(webrtc.SignalingHandler(fp))
	defer srv.Close()
	offer := webrtc.SDP{SessionID: "8", Setup: "actpass", Mid: "0", IceUfrag: "off8", IcePwd: "offererpassword8", Fingerprint: "01:23:45:67:89:AB:CD:EF:01:23:45:67:89:AB:CD:EF"}
	resp, err := http.Post(srv.URL+"/signal", "application/sdp", strings.NewReader(offer.Marshal()))
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	ans, err := webrtc.ParseSDP(string(raw))
	if err != nil {
		return "", fmt.Errorf("answer unparseable: %w", err)
	}
	if ans.Setup != "active" || ans.Fingerprint != fp {
		return "", fmt.Errorf("incoherent answer: setup=%s", ans.Setup)
	}
	return "SDP offer→answer CALL negotiated (role flipped actpass→active, peer fingerprint bound)", nil
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
