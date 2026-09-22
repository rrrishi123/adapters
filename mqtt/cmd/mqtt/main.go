// mqtt — the persistent broker relay as a single portable binary.
//
//	mqtt -role host -broker mqtt://broker:1883 -prefix wire -node mac -collector http://127.0.0.1:7070 -actor mac
//	mqtt -role far  -broker mqtt://broker:1883 -prefix wire -node sandbox -call GET http://127.0.0.1:7070/status
//	mqtt -role far  -broker ... -propose-file envelope.json
//	mqtt -role far  -broker ... -await <ulid>          # fetch the (retained) receipt for a ULID
//	mqtt -role far  -broker ... -channel fox browsingContext.getTree '{}'
//	mqtt -role broker -listen 127.0.0.1:1883        # the in-process broker as a daemon (no mosquitto needed)
//
// Every value is a flag with an MQTT_* environment fallback; nothing points at
// a machine unless the operator says so. Both roles only dial OUT. The host's
// policy is a JSON file (missing = open, malformed = closed); its auth slots
// file is never published anywhere.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/rrrishi123/adapters/internal/httpx"
	"github.com/rrrishi123/adapters/mqtt"
)

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func envDuration(key string, def time.Duration) time.Duration {
	if v := os.Getenv(key); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			return d
		}
	}
	return def
}

func envInt(key string, def int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}

func envBool(key string) bool {
	switch strings.ToLower(os.Getenv(key)) {
	case "1", "true", "yes", "on":
		return true
	}
	return false
}

func main() {
	var (
		role      = flag.String("role", envOr("MQTT_ROLE", ""), "host | far | broker (required)")
		listen    = flag.String("listen", envOr("MQTT_LISTEN", "127.0.0.1:1883"), "broker: address to serve on")
		broker    = flag.String("broker", envOr("MQTT_BROKER", ""), "broker URL, mqtt://host:1883 or mqtts://host:8883 (required)")
		prefix    = flag.String("prefix", envOr("MQTT_PREFIX", "wire"), "topic prefix")
		node      = flag.String("node", envOr("MQTT_NODE", ""), "this node's name (client id + presence level); default hostname")
		username  = flag.String("username", envOr("MQTT_USERNAME", ""), "broker username")
		password  = flag.String("password", envOr("MQTT_PASSWORD", ""), "broker password")
		caFile    = flag.String("ca", envOr("MQTT_CA", ""), "PEM CA bundle for mqtts://")
		insecure  = flag.Bool("tls-insecure", envBool("MQTT_TLS_INSECURE"), "skip TLS certificate verification (lab brokers only)")
		qos       = flag.Int("qos", envInt("MQTT_QOS", 1), "QoS for commands and receipts (0 or 1)")
		keepAlive = flag.Duration("keepalive", envDuration("MQTT_KEEPALIVE", 30*time.Second), "MQTT keep-alive")
		reconnect = flag.Duration("reconnect", envDuration("MQTT_RECONNECT", 3*time.Second), "host: back-off between sessions")
		collector = flag.String("collector", envOr("MQTT_COLLECTOR", ""), "host: witness base URL, e.g. http://127.0.0.1:7070")
		actor     = flag.String("actor", envOr("MQTT_ACTOR", ""), "host: X-8-Actor declared on every fire; default node")
		policy    = flag.String("policy", envOr("MQTT_POLICY", ""), "host: policy JSON file (missing = open, malformed = closed); empty = allow-all")
		slots     = flag.String("slots", envOr("MQTT_SLOTS", ""), "host: auth-slot file; never published")
		maxBody   = flag.Int("max-body", envInt("MQTT_MAX_BODY", 8192), "host: receipt body cap in bytes")
		fireTO    = flag.Duration("fire-timeout", envDuration("MQTT_FIRE_TIMEOUT", 60*time.Second), "host: default per-fire deadline")

		proposeFile = flag.String("propose-file", "", "far: envelope JSON file to propose ('-' = stdin)")
		propose     = flag.String("propose", "", "far: envelope JSON literal to propose")
		await       = flag.String("await", "", "far: only wait for the receipt of this ULID (retained or live)")
		call        = flag.Bool("call", false, "far: propose a CALL from the positional args: METHOD URL [BODY]")
		channel     = flag.Bool("channel", false, "far: propose a CHANNEL command from the positional args: SEAT METHOD [PARAMS]")
		ulid        = flag.String("ulid", "", "far: ULID for -call/-channel (default: time-based)")
		why         = flag.String("why", "", "far: free text echoed into the receipt")
		wait        = flag.Duration("wait", envDuration("MQTT_WAIT", 90*time.Second), "far: how long to wait for the receipt")
		verbose     = flag.Bool("v", envBool("MQTT_VERBOSE"), "log to stderr")
	)
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "usage:\n  mqtt -role host -broker URL [-prefix P] [-node N] -collector URL [-actor A] [-policy F] [-slots F]\n  mqtt -role far  -broker URL [-prefix P] [-node N] -call METHOD URL [BODY] | -channel SEAT METHOD [PARAMS] | -propose JSON | -propose-file F | -await ULID\n  mqtt -role broker [-listen ADDR] [-username U -password P]\n\nflags (each falls back to MQTT_<NAME>):\n")
		flag.PrintDefaults()
	}
	flag.Parse()

	var logger *log.Logger
	if *verbose || *role == "host" {
		logger = log.New(os.Stderr, "mqtt ", log.LstdFlags)
	}
	cfg := mqtt.Config{
		Broker: *broker, Prefix: *prefix, Node: *node, Username: *username, Password: *password,
		CAFile: *caFile, TLSInsecure: *insecure, KeepAlive: *keepAlive, QoS: byte(*qos), Reconnect: *reconnect,
		Collector: *collector, Actor: *actor, MaxBody: *maxBody, FireTimeout: *fireTO, Slots: *slots, Log: logger,
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if *role == "broker" {
		br, err := mqtt.Listen(*listen)
		if err != nil {
			log.Fatal(err)
		}
		br.Log = log.New(os.Stderr, "mqtt ", log.LstdFlags)
		if *username != "" || *password != "" {
			br.Auth = func(u, p string) bool { return u == *username && p == *password }
		}
		fmt.Println(br.URL())
		<-ctx.Done()
		br.Close()
		return
	}
	if *broker == "" {
		fmt.Fprintln(os.Stderr, "mqtt: -broker is required (or MQTT_BROKER)")
		flag.Usage()
		os.Exit(2)
	}

	switch *role {
	case "host":
		h := &mqtt.Host{Config: cfg, Log: logger}
		if *policy != "" {
			h.Policy = mqtt.FilePolicy{Path: *policy}
		}
		if err := h.Run(ctx); err != nil && err != context.Canceled {
			log.Fatal(err)
		}
		st := h.Stats()
		_ = json.NewEncoder(os.Stderr).Encode(st)

	case "far":
		f := &mqtt.FarEnd{Config: cfg, Log: logger}
		wctx, cancel := context.WithTimeout(ctx, *wait)
		defer cancel()
		if *await != "" {
			c, err := f.Connect(wctx)
			if err != nil {
				log.Fatal(err)
			}
			rec, err := f.Await(wctx, c, *await)
			c.Close() // graceful DISCONNECT before exit: no will fires
			os.Exit(emit(rec, err))
		}
		env, err := buildEnvelope(flag.Args(), *call, *channel, *propose, *proposeFile, *ulid, *why)
		if err != nil {
			fmt.Fprintln(os.Stderr, "mqtt:", err)
			flag.Usage()
			os.Exit(2)
		}
		rec, err := f.Fire(wctx, env) // Fire closes its own session
		os.Exit(emit(rec, err))

	default:
		fmt.Fprintln(os.Stderr, "mqtt: -role must be host, far or broker (or MQTT_ROLE)")
		flag.Usage()
		os.Exit(2)
	}
}

// buildEnvelope turns the far end's CLI form into an envelope:
//
//	-propose '<json>' | -propose-file f | -call METHOD URL [BODY] | -channel SEAT METHOD [PARAMS]
func buildEnvelope(args []string, call, channel bool, literal, file, ulid, why string) (*mqtt.Envelope, error) {
	var env mqtt.Envelope
	switch {
	case literal != "":
		if err := json.Unmarshal([]byte(literal), &env); err != nil {
			return nil, fmt.Errorf("-propose: %w", err)
		}
	case file != "":
		var raw []byte
		var err error
		if file == "-" {
			raw, err = readAll(os.Stdin)
		} else {
			raw, err = os.ReadFile(file)
		}
		if err != nil {
			return nil, err
		}
		if err := json.Unmarshal(raw, &env); err != nil {
			return nil, fmt.Errorf("-propose-file: %w", err)
		}
	case call && len(args) >= 2:
		env.Atom = mqtt.AtomCall
		env.Call = &httpx.Request{Method: strings.ToUpper(args[0]), URL: args[1]}
		if len(args) > 2 {
			env.Call.Body = args[2]
		}
	case channel && len(args) >= 2:
		env.Atom = mqtt.AtomChannel
		env.Channel = &mqtt.ChannelOp{Session: args[0], Method: args[1]}
		if len(args) > 2 {
			env.Channel.Params = json.RawMessage(args[2])
		}
	default:
		return nil, fmt.Errorf("far end needs -call METHOD URL [BODY], -channel SEAT METHOD [PARAMS], -propose JSON, -propose-file F, or -await ULID")
	}
	if ulid != "" {
		env.ULID = ulid
	}
	if env.ULID == "" {
		env.ULID = fmt.Sprintf("%d-%d", time.Now().UTC().UnixMilli(), os.Getpid())
	}
	if why != "" {
		env.Why = why
	}
	if env.Seq == 0 {
		env.Seq = time.Now().UnixNano()
	}
	return &env, nil
}

func readAll(f *os.File) ([]byte, error) {
	var out []byte
	buf := make([]byte, 4096)
	for {
		n, err := f.Read(buf)
		out = append(out, buf[:n]...)
		if err != nil {
			if err.Error() == "EOF" {
				return out, nil
			}
			return out, err
		}
	}
}

// emit prints the receipt as one JSON line; the exit code is 1 unless it fired cleanly.
func emit(rec *mqtt.Receipt, err error) int {
	if err != nil {
		fmt.Fprintln(os.Stderr, "mqtt:", err)
		return 1
	}
	_ = json.NewEncoder(os.Stdout).Encode(rec)
	if !rec.Decision.Fired || rec.Error != "" {
		return 1
	}
	return 0
}
