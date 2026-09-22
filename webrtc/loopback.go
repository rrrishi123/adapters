package webrtc

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"sync/atomic"
	"time"
)

// Loopback owns both peers: localhost HTTP offer/answer, loopback-only ICE,
// real DTLS/SCTP, and multiple bidi_command-shaped exchanges on one CHANNEL.
func Loopback(ctx context.Context) (string, error) {
	serverErrors := make(chan error, 1)
	handler, err := NewSignaler(Config{LoopbackOnly: true}, func(c *Conn) {
		serverErrors <- Echo(c, 5*time.Second)
	})
	if err != nil {
		return "", err
	}
	defer handler.Close()
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		handler.ServeHTTP(w, r)
	}))
	defer srv.Close()
	c, err := Dial(ctx, Config{SignalingURL: srv.URL + "/signal", LoopbackOnly: true})
	if err != nil {
		return "", err
	}
	defer c.Close()
	// Prove this is a held CHANNEL, independent of the one signaling CALL.
	srv.Close()
	if err := c.SetDeadline(time.Now().Add(5 * time.Second)); err != nil {
		return "", err
	}
	events := 0
	for id := 1; id <= 3; id++ {
		payload := fmt.Sprintf("wire-%d", id)
		response, err := c.BidiCommand(id, "echo", map[string]any{"text": payload}, func(event string) {
			var e struct {
				Method string `json:"method"`
			}
			if json.Unmarshal([]byte(event), &e) == nil && e.Method == "channel.ready" {
				events++
			}
		})
		if err != nil {
			return "", err
		}
		var got struct {
			ID     int `json:"id"`
			Result struct {
				Text string `json:"text"`
			} `json:"result"`
		}
		if json.Unmarshal(response, &got) != nil || got.ID != id || got.Result.Text != payload {
			return "", fmt.Errorf("webrtc: bad CHANNEL response: %s", response)
		}
	}
	if calls.Load() != 1 || events != 1 {
		return "", fmt.Errorf("webrtc: signaling CALLs=%d, peer events=%d; want 1 each", calls.Load(), events)
	}
	if err := c.Close(); err != nil {
		return "", err
	}
	select {
	case err := <-serverErrors:
		// The echo peer ends when the channel closes (EOF/transport closure).
		if errors.Is(err, os.ErrDeadlineExceeded) {
			return "", fmt.Errorf("webrtc: peer timed out instead of observing channel close: %w", err)
		}
	case <-ctx.Done():
		return "", fmt.Errorf("webrtc: peer did not close: %w", ctx.Err())
	case <-time.After(6 * time.Second):
		return "", fmt.Errorf("webrtc: peer did not close")
	}
	return "CHANNEL (bidi_command): 3 matched commands + 1 peer event over real ICE/DTLS/SCTP DataChannel; 1 httpx offer/answer CALL on localhost; signaling server stopped before commands; loopback candidates only, no external STUN", nil
}

// Echo is an explicit demonstration peer, not a dispatcher to host tools.
// Applications provide their own channel handler to NewSignaler.
func Echo(c *Conn, timeout time.Duration) error {
	if err := c.SetDeadline(time.Now().Add(timeout)); err != nil {
		return err
	}
	if err := c.WriteText(`{"method":"channel.ready","params":{}}`); err != nil {
		return err
	}
	for {
		if err := c.SetDeadline(time.Now().Add(timeout)); err != nil {
			return err
		}
		frame, err := c.ReadText()
		if err != nil {
			return err
		}
		var command struct {
			ID     *int            `json:"id"`
			Method string          `json:"method"`
			Params json.RawMessage `json:"params"`
		}
		if err := json.Unmarshal([]byte(frame), &command); err != nil || command.ID == nil {
			return fmt.Errorf("webrtc: invalid command frame")
		}
		response := map[string]any{"id": *command.ID}
		if command.Method != "echo" {
			response["error"] = "unknown command"
		} else {
			if len(command.Params) == 0 {
				command.Params = json.RawMessage(`{}`)
			}
			response["result"] = command.Params
		}
		raw, err := json.Marshal(response)
		if err != nil {
			return err
		}
		if err := c.WriteText(string(raw)); err != nil {
			return err
		}
	}
}
