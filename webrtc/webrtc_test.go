package webrtc

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	pion "github.com/pion/webrtc/v4"
)

func TestLoopbackDataChannel(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	detail, err := Loopback(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Log(detail)
}

func pair(t *testing.T) (*Conn, *Conn, *Signaler) {
	t.Helper()
	accepted := make(chan *Conn, 1)
	s, err := NewSignaler(Config{LoopbackOnly: true, Timeout: 3 * time.Second}, func(c *Conn) {
		accepted <- c
		<-c.done
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	srv := httptest.NewServer(s)
	t.Cleanup(srv.Close)
	c, err := Dial(context.Background(), Config{SignalingURL: srv.URL, LoopbackOnly: true, Timeout: 3 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Close() })
	var peer *Conn
	select {
	case peer = <-accepted:
	case <-time.After(3 * time.Second):
		t.Fatal("answerer did not open")
	}
	for _, conn := range []*Conn{c, peer} {
		if err := conn.SetDeadline(time.Now().Add(3 * time.Second)); err != nil {
			t.Fatal(err)
		}
		if conn.pc.ConnectionState() != pion.PeerConnectionStateConnected {
			t.Fatal("peer not connected")
		}
		if conn.pc.SCTP().Transport().State() != pion.DTLSTransportStateConnected {
			t.Fatal("DTLS not connected")
		}
		transport := conn.pc.SCTP().Transport().ICETransport()
		candidates, err := transport.GetSelectedCandidatePair()
		if err != nil || candidates == nil {
			t.Fatalf("missing selected ICE pair: %v", err)
		}
		if candidates.Local.Address != "127.0.0.1" || candidates.Remote.Address != "127.0.0.1" {
			t.Fatalf("non-loopback ICE selected: %+v", candidates)
		}
	}
	return c, peer, s
}

func TestMessageBoundariesAndConcurrentWriters(t *testing.T) {
	c, peer, _ := pair(t)
	messages := []string{"", "hello λ 世界", strings.Repeat("x", maxMessageSize)}
	for _, want := range messages {
		if err := c.WriteText(want); err != nil {
			t.Fatal(err)
		}
		got, err := peer.ReadText()
		if err != nil || got != want {
			t.Fatalf("message boundary: len=%d want=%d err=%v", len(got), len(want), err)
		}
	}
	if err := c.WriteText(strings.Repeat("x", maxMessageSize+1)); err == nil {
		t.Fatal("accepted oversized message")
	}
	var wg sync.WaitGroup
	errs := make(chan error, 12)
	for i := 0; i < 12; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			errs <- c.WriteText(strings.Repeat(string(rune('a'+i)), 4000))
		}(i)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	seen := map[byte]bool{}
	for i := 0; i < 12; i++ {
		frame, err := peer.ReadText()
		if err != nil {
			t.Fatal(err)
		}
		if len(frame) != 4000 || frame != strings.Repeat(frame[:1], 4000) || seen[frame[0]] {
			t.Fatal("interleaved or duplicated concurrent write")
		}
		seen[frame[0]] = true
	}
	// A binary frame is not returned as a command/event text frame.
	raw, err := peer.channel()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := raw.WriteDataChannel([]byte("binary"), false); err != nil {
		t.Fatal(err)
	}
	if err := peer.WriteText("text"); err != nil {
		t.Fatal(err)
	}
	if got, err := c.ReadText(); err != nil || got != "text" {
		t.Fatalf("binary skip: %q %v", got, err)
	}
}

func TestDeadlineResetAndClose(t *testing.T) {
	c, peer, s := pair(t)
	if err := c.SetDeadline(time.Now().Add(30 * time.Millisecond)); err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	if _, err := c.ReadText(); err == nil {
		t.Fatal("read did not time out")
	}
	if time.Since(start) > time.Second {
		t.Fatal("read deadline ignored")
	}
	if err := c.SetDeadline(time.Now().Add(3 * time.Second)); err != nil {
		t.Fatal(err)
	}
	if err := peer.WriteText("after timeout"); err != nil {
		t.Fatal(err)
	}
	if got, err := c.ReadText(); err != nil || got != "after timeout" {
		t.Fatalf("deadline reset: %q %v", got, err)
	}
	pending := make(chan error, 1)
	go func() { _, err := c.ReadText(); pending <- err }()
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-pending:
		if err == nil {
			t.Fatal("close produced successful read")
		}
	case <-time.After(time.Second):
		t.Fatal("Close did not unblock read")
	}
	if err := c.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
	if err := c.WriteText("closed"); !errors.Is(err, io.ErrClosedPipe) {
		t.Fatalf("write after Close: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-peer.done:
	default:
		t.Fatal("signaler Close did not close peer")
	}
}

func TestCommandMatchesIDAndDeliversPeerEvents(t *testing.T) {
	c, peer, _ := pair(t)
	done := make(chan error, 1)
	go func() {
		request, err := peer.ReadText()
		if err != nil {
			done <- err
			return
		}
		var cmd struct {
			ID     int            `json:"id"`
			Method string         `json:"method"`
			Params map[string]any `json:"params"`
		}
		if err := json.Unmarshal([]byte(request), &cmd); err != nil {
			done <- err
			return
		}
		if cmd.ID != 0 || cmd.Method != "test" || cmd.Params == nil {
			done <- errors.New("bad command shape")
			return
		}
		for _, frame := range []string{`{"method":"event"}`, `{"id":99,"result":{}}`, `{"id":0,"result":{"ok":true}}`} {
			if err := peer.WriteText(frame); err != nil {
				done <- err
				return
			}
		}
		done <- nil
	}()
	var events []string
	response, err := c.BidiCommand(0, "test", nil, func(event string) { events = append(events, event) })
	if err != nil {
		t.Fatal(err)
	}
	if string(response) != `{"id":0,"result":{"ok":true}}` || len(events) != 2 {
		t.Fatalf("response=%s events=%v", response, events)
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func TestSignalingRejectsInvalidRequests(t *testing.T) {
	s, err := NewSignaler(Config{LoopbackOnly: true}, func(*Conn) { t.Error("invalid offer opened a channel") })
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	for _, tc := range []struct {
		name, method, contentType, body string
		status                          int
	}{
		{"method", "GET", "", "", 405},
		{"content type", "POST", "application/json", `{}`, 415},
		{"media only", "POST", "application/sdp", "v=0\r\nm=audio 9 UDP/TLS/RTP/SAVPF 111\r\n", 400},
		{"malformed", "POST", "application/sdp", "webrtc-datachannel", 400},
		{"oversized", "POST", "application/sdp", strings.Repeat("x", maxSDPSize+1), 400},
	} {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest(tc.method, "/signal", strings.NewReader(tc.body))
			req.Header.Set("Content-Type", tc.contentType)
			w := httptest.NewRecorder()
			s.ServeHTTP(w, req)
			if w.Code != tc.status {
				t.Fatalf("status=%d want=%d", w.Code, tc.status)
			}
			s.mu.Lock()
			defer s.mu.Unlock()
			if len(s.peers) != 0 {
				t.Fatal("rejected offer leaked a peer")
			}
		})
	}
}

func TestDialRejectsBadAnswersAndHonorsCancellation(t *testing.T) {
	for _, tc := range []struct {
		name   string
		status int
		body   string
	}{
		{"rejected", 403, "no"},
		{"invalid SDP", 200, "not SDP"},
		{"redirect", 307, ""},
		{"oversized", 200, strings.Repeat("x", maxSDPSize+1)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method != "POST" || r.Header.Get("Content-Type") != "application/sdp" {
					t.Error("not an SDP signaling CALL")
				}
				_, _ = io.Copy(io.Discard, r.Body)
				w.Header().Set("Location", "/elsewhere")
				w.WriteHeader(tc.status)
				_, _ = io.WriteString(w, tc.body)
			}))
			defer srv.Close()
			if c, err := Dial(context.Background(), Config{SignalingURL: srv.URL, LoopbackOnly: true, Timeout: time.Second}); err == nil {
				c.Close()
				t.Fatal("bad answer accepted")
			}
		})
	}
	t.Run("context cancellation in httpx CALL", func(t *testing.T) {
		seen := make(chan struct{})
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			_, _ = io.Copy(io.Discard, r.Body)
			close(seen)
			<-r.Context().Done()
		}))
		defer srv.Close()
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		done := make(chan error, 1)
		go func() {
			c, err := Dial(ctx, Config{SignalingURL: srv.URL, LoopbackOnly: true, Timeout: 3 * time.Second})
			if c != nil {
				c.Close()
			}
			done <- err
		}()
		select {
		case <-seen:
		case <-time.After(time.Second):
			t.Fatal("no signaling CALL")
		}
		cancel()
		select {
		case err := <-done:
			if !errors.Is(err, context.Canceled) {
				t.Fatalf("cancellation: %v", err)
			}
		case <-time.After(time.Second):
			t.Fatal("cancellation did not interrupt signaling")
		}
	})
}

func TestConfigHasNoDeploymentDefaults(t *testing.T) {
	if _, err := Dial(context.Background(), Config{}); err == nil {
		t.Fatal("missing signaling endpoint accepted")
	}
	if _, err := NewSignaler(Config{LoopbackOnly: true, ICEServers: []ICEServer{{URLs: []string{"stun:example.invalid:3478"}}}}, func(*Conn) {}); err == nil {
		t.Fatal("loopback-only accepted external ICE server")
	}
	if _, err := NewSignaler(Config{Timeout: -time.Second}, func(*Conn) {}); err == nil {
		t.Fatal("negative timeout accepted")
	}
}
