package grpc

// Own-both-ends validation of the gRPC adapter shape over REAL HTTP/2: we stand
// up the service on an http/2 test server, then drive unary (CALL) and
// server-stream (CHANNEL) through gRPC framing and assert both round-trip.
// httptest with EnableHTTP2 + StartTLS gives a genuine h2 server; srv.Client()
// negotiates h2 via ALPN — so this exercises the transport the wire actually
// delegates to grpc, not an http/1 stand-in.

import (
	"fmt"
	"net/http/httptest"
	"testing"
)

func newH2Server(t *testing.T) (*httptest.Server, *Client) {
	t.Helper()
	srv := httptest.NewUnstartedServer(Handler())
	srv.EnableHTTP2 = true
	srv.StartTLS()
	c := &Client{HC: srv.Client(), Base: srv.URL}
	return srv, c
}

func TestGRPC_Unary_isCALL(t *testing.T) {
	srv, c := newH2Server(t)
	defer srv.Close()

	got, err := c.Unary("hello")
	if err != nil {
		t.Fatalf("unary: %v", err)
	}
	if got != "echo:hello" {
		t.Fatalf("unary CALL round-trip: got %q, want %q", got, "echo:hello")
	}

	// confirm we actually negotiated HTTP/2, not http/1.1 — gRPC requires h2.
	resp, err := c.HC.Get(srv.URL + "/wire.Echo/Unary")
	if err == nil {
		defer resp.Body.Close()
		if resp.ProtoMajor != 2 {
			t.Fatalf("expected HTTP/2, negotiated %s", resp.Proto)
		}
	}
}

func TestGRPC_ServerStream_isCHANNEL(t *testing.T) {
	srv, c := newH2Server(t)
	defer srv.Close()

	frames, err := c.Stream("tick")
	if err != nil {
		t.Fatalf("stream: %v", err)
	}
	if len(frames) != StreamCount {
		t.Fatalf("CHANNEL frame count: got %d, want %d (%v)", len(frames), StreamCount, frames)
	}
	for i, f := range frames {
		want := fmt.Sprintf("tick#%d", i)
		if f != want {
			t.Fatalf("frame %d: got %q, want %q", i, f, want)
		}
	}
}
