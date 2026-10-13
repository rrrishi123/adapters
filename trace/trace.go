// Package trace defines the NEUTRAL trace -- the contract between 8 (recorder)
// and adapters (replayer). v0.0.1 review: the replay runner does not yet exist;
// the LambdaTest matrix runner (lambdatest/cmd/matrix/) does not consume this
// neutral format -- it hardcodes its own JSON schema. The two should converge.
//
// Package trace defines the NEUTRAL, provider-agnostic session trace — the
// contract between 8 (the witness, which RECORDS) and an adapter's replay runner
// (which MATERIALIZES the trace into a provider-specific, replayable suite).
//
// Design (locked with the peer):
//   - 8 observes the wire across every session and emits Frames. It never learns
//     which provider a trace came from — it records raw wire events only.
//   - An adapter reads the trace and re-executes it through http-mcp's two MODES
//     (CALL = http_request, CHANNEL = bidi_command), interpreting placeholders and
//     provider shape through its own spec.json.
//   - A Frame NEVER carries a credential. Authorization is stripped to an AuthSlot
//     naming WHERE the resolved profile credential must be injected at replay time.
//     The secret stays below the boundary — the trace is safe to store and share.
//
// Wire format: NDJSON (one Frame per line) so it streams and appends cheaply.
package trace

import (
	"bufio"
	"encoding/json"
	"io"
	"strings"
)

// Version is the contract version this package speaks. The shared README
// ("Versioning") promises a separately-versioned contract that each arm
// declares and the replay runner checks before replaying — this constant is
// that version. Every Frame a Writer emits is stamped with it (Contract), and
// Compatible tells a reader whether a trace's stamp is one it can replay.
// Baseline v0.0.2, matching the documented baseline across all four arms.
//
// TODO(contract-dedup): http-mcp/contract.Version is the canonical constant;
// this literal duplicates it because adapters is a zero-dependency module and
// importing http-mcp/contract would add a new adapters→http-mcp module arrow.
// Until that arrow is decided, bump BOTH in lockstep.
const Version = "v0.0.2"

// Compatible reports whether a trace stamped v can be replayed by this
// package: same major.minor. A missing stamp (a pre-v0.0.2 trace) is
// compatible — the format did not change, only the stamp arrived.
func Compatible(v string) bool {
	if v == "" {
		return true
	}
	return majorMinor(v) == majorMinor(Version)
}

func majorMinor(v string) string {
	v = strings.TrimPrefix(v, "v")
	if i := strings.LastIndexByte(v, '.'); i > 0 {
		return v[:i]
	}
	return v
}

// Mode is the interaction mode an atom uses (not a transport — transports are
// dialects of these two). CALL = one request -> one response. CHANNEL = a held
// duplex connection producing commands and consuming events.
const (
	ModeCall    = "call"
	ModeChannel = "channel"
)

// Dir is the direction of a frame relative to the model/host. EFFERENT flows
// toward the target (an act); AFFERENT flows back (an observation). The model
// learns only from afferent frames — an act is known by its afferent result.
const (
	DirEfferent = "efferent"
	DirAfferent = "afferent"
)

// Frame is one neutral event in a recorded session.
type Frame struct {
	Contract string `json:"contract,omitempty"` // Version at emission; a reader checks Compatible
	Seq      int    `json:"seq"`                // monotonic order within a session
	TS       int64  `json:"ts"`                 // unix millis at observation
	Session  string `json:"session"`            // opaque id for one held context / build
	Mode     string `json:"mode"`               // ModeCall | ModeChannel
	Dir      string `json:"dir"`                // DirEfferent | DirAfferent

	// CALL mode
	Method string `json:"method,omitempty"` // efferent: HTTP method
	URL    string `json:"url,omitempty"`    // efferent: target; may carry {placeholders}
	Status int    `json:"status,omitempty"` // afferent: HTTP status

	// CHANNEL mode
	Command string `json:"command,omitempty"` // efferent: protocol method (CDP/BiDi/...)
	Event   string `json:"event,omitempty"`   // afferent: event/result name

	// shared
	Headers  map[string]string `json:"headers,omitempty"`   // auth-stripped
	Body     json.RawMessage   `json:"body,omitempty"`      // request/response/params/result
	AuthSlot string            `json:"auth_slot,omitempty"` // names WHERE a credential injects; never the secret
}

// Writer emits frames as NDJSON.
type Writer struct {
	w   io.Writer
	seq int
}

// NewWriter wraps w for NDJSON frame emission.
func NewWriter(w io.Writer) *Writer { return &Writer{w: w} }

// Emit assigns the next sequence number, stamps the contract Version, and
// writes the frame as one JSON line. The caller sets Mode/Dir/payload; Seq
// and Contract are owned by the Writer.
func (e *Writer) Emit(f Frame) error {
	f.Contract = Version
	f.Seq = e.seq
	e.seq++
	b, err := json.Marshal(f)
	if err != nil {
		return err
	}
	if _, err := e.w.Write(append(b, '\n')); err != nil {
		return err
	}
	return nil
}

// Read consumes an NDJSON trace into frames. Blank lines are skipped so a trace
// can be concatenated or partially flushed without breaking the reader.
func Read(r io.Reader) ([]Frame, error) {
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 0, 64*1024), 8*1024*1024)
	var frames []Frame
	for sc.Scan() {
		line := sc.Bytes()
		if len(line) == 0 {
			continue
		}
		var f Frame
		if err := json.Unmarshal(line, &f); err != nil {
			return frames, err
		}
		frames = append(frames, f)
	}
	return frames, sc.Err()
}
