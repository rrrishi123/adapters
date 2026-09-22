package main

import (
	"bytes"
	"context"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/rrrishi123/adapters/webrtc"
)

func TestNodeEnvironmentAndFlagOverrides(t *testing.T) {
	s, err := webrtc.NewSignaler(webrtc.Config{LoopbackOnly: true}, func(c *webrtc.Conn) { _ = webrtc.Echo(c, time.Second) })
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	srv := httptest.NewServer(s)
	defer srv.Close()
	t.Setenv("WEBRTC_ROLE", "dial")
	t.Setenv("WEBRTC_SIGNALING", "invalid-overridden-by-flag")
	t.Setenv("WEBRTC_ICE_SERVERS", "invalid-overridden-by-flag")
	t.Setenv("WEBRTC_LOOPBACK_ONLY", "true")
	t.Setenv("WEBRTC_LABEL", "wire")
	t.Setenv("WEBRTC_TIMEOUT", "3s")
	t.Setenv("WEBRTC_METHOD", "echo")
	t.Setenv("WEBRTC_PARAMS", `{"text":"configured"}`)
	var out, events bytes.Buffer
	if err := run(context.Background(), []string{"-signaling", srv.URL, "-ice-servers", "[]"}, &out, &events); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out.String(), `"text":"configured"`) || !strings.Contains(events.String(), "channel.ready") {
		t.Fatalf("response=%s events=%s", &out, &events)
	}
}

func TestInvalidNodeConfiguration(t *testing.T) {
	for _, args := range [][]string{
		{"-role", "answer", "-listen", "", "-signal-path", ""},
		{"-role", "dial", "-timeout", "invalid"},
		{"-role", "dial", "-ice-servers", "invalid"},
		{"-role", "dial", "-loopback-only", "invalid"},
	} {
		var out bytes.Buffer
		if err := run(context.Background(), args, &out, &out); err == nil {
			t.Fatalf("accepted %v", args)
		}
	}
}
