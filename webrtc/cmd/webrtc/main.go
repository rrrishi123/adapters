// webrtc — an experimental transport primitive: echo peer or CHANNEL client.
// Endpoints are required flags/WEBRTC_ environment values; there are no
// default signaling addresses or public STUN servers.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/rrrishi123/adapters/webrtc"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := run(ctx, os.Args[1:], os.Stdout, os.Stderr); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(ctx context.Context, args []string, out, diagnostic io.Writer) error {
	// The stdlib-only parent loopback command invokes this separate binary.
	// This self-contained demo ignores deployment flags and environment values.
	if len(args) == 1 && args[0] == "loopback" {
		ctx, cancel := context.WithTimeout(ctx, 15*time.Second)
		defer cancel()
		detail, err := webrtc.Loopback(ctx)
		if err != nil {
			return err
		}
		_, err = fmt.Fprintln(out, detail)
		return err
	}
	fs := flag.NewFlagSet("webrtc", flag.ContinueOnError)
	fs.SetOutput(diagnostic)
	role := fs.String("role", os.Getenv("WEBRTC_ROLE"), "answer | dial (required); answer is an explicit echo demo")
	signaling := fs.String("signaling", os.Getenv("WEBRTC_SIGNALING"), "dial: full signaling HTTP(S) endpoint (required)")
	listen := fs.String("listen", os.Getenv("WEBRTC_LISTEN"), "answer: bind address (required)")
	path := fs.String("signal-path", os.Getenv("WEBRTC_SIGNAL_PATH"), "answer: signaling HTTP path (required)")
	iceJSON := fs.String("ice-servers", os.Getenv("WEBRTC_ICE_SERVERS"), "ICE server JSON array, e.g. [{\"urls\":[\"stun:host:3478\"]}]; empty = host candidates only")
	label := fs.String("label", envOr("WEBRTC_LABEL", "wire"), "DataChannel label (both nodes must match)")
	timeoutText := fs.String("timeout", envOr("WEBRTC_TIMEOUT", "15s"), "negotiation/command timeout")
	loopbackText := fs.String("loopback-only", envOr("WEBRTC_LOOPBACK_ONLY", "false"), "true restricts ICE to localhost; cannot combine with ICE servers")
	method := fs.String("method", os.Getenv("WEBRTC_METHOD"), "dial: bidi_command method (required)")
	paramsText := fs.String("params", envOr("WEBRTC_PARAMS", "{}"), "dial: command params JSON object")
	id := fs.Int("id", 1, "dial: command id")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 0 {
		return errors.New("webrtc: unexpected positional arguments")
	}
	timeout, err := time.ParseDuration(*timeoutText)
	if err != nil || timeout <= 0 {
		return errors.New("webrtc: timeout must be a positive duration")
	}
	var loopback bool
	switch *loopbackText {
	case "true", "1":
		loopback = true
	case "false", "0":
	default:
		return errors.New("webrtc: loopback-only must be true or false")
	}
	var servers []webrtc.ICEServer
	if *iceJSON != "" {
		if err := json.Unmarshal([]byte(*iceJSON), &servers); err != nil {
			return fmt.Errorf("webrtc: ice-servers: %w", err)
		}
	}
	cfg := webrtc.Config{SignalingURL: *signaling, ICEServers: servers, Label: *label, Timeout: timeout, LoopbackOnly: loopback}
	switch *role {
	case "answer":
		if *listen == "" || !strings.HasPrefix(*path, "/") || strings.ContainsAny(*path, " ?#{}") {
			return errors.New("webrtc: answer requires -listen and a literal -signal-path (or WEBRTC_LISTEN/WEBRTC_SIGNAL_PATH)")
		}
		handler, err := webrtc.NewSignaler(cfg, func(c *webrtc.Conn) { _ = webrtc.Echo(c, timeout) })
		if err != nil {
			return err
		}
		defer handler.Close()
		ln, err := net.Listen("tcp", *listen)
		if err != nil {
			return err
		}
		defer ln.Close()
		server := &http.Server{ReadTimeout: timeout, ReadHeaderTimeout: timeout, WriteTimeout: 2 * timeout,
			Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path != *path {
					http.NotFound(w, r)
					return
				}
				handler.ServeHTTP(w, r)
			})}
		defer server.Close()
		stopped := make(chan error, 1)
		go func() { stopped <- server.Serve(ln) }()
		fmt.Fprintf(out, "http://%s%s\n", ln.Addr(), *path)
		select {
		case <-ctx.Done():
			return nil
		case err := <-stopped:
			return err
		}
	case "dial":
		if *method == "" {
			return errors.New("webrtc: dial requires -method (or WEBRTC_METHOD)")
		}
		var params map[string]any
		if err := json.Unmarshal([]byte(*paramsText), &params); err != nil || params == nil {
			return errors.New("webrtc: params must be a JSON object")
		}
		c, err := webrtc.Dial(ctx, cfg)
		if err != nil {
			return err
		}
		defer c.Close()
		stopClose := context.AfterFunc(ctx, func() { _ = c.Close() })
		defer stopClose()
		if err := c.SetDeadline(time.Now().Add(timeout)); err != nil {
			return err
		}
		response, err := c.BidiCommand(*id, *method, params, func(event string) { fmt.Fprintln(diagnostic, event) })
		if err != nil {
			return err
		}
		_, err = fmt.Fprintln(out, string(response))
		return err
	default:
		return errors.New("webrtc: -role must be answer or dial (or WEBRTC_ROLE)")
	}
}

func envOr(key, fallback string) string {
	if value, ok := os.LookupEnv(key); ok {
		return value
	}
	return fallback
}
