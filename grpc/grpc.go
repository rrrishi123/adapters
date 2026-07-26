// Package grpc is the gRPC adapter's own-both-ends conformance harness: a real
// gRPC-framed service over real HTTP/2, in pure stdlib — no protoc, no grpc lib.
//
// It proves the transport shape the wire delegates here:
//
//	unary        = CALL     — one request frame, one response frame (http_request)
//	server-stream= CHANNEL  — one request frame, N response frames (bidi_command)
//
// gRPC is HTTP/2 + a trivial framing: each message is [1 compressed-flag byte]
// [4 big-endian length][protobuf bytes], and the call's outcome rides an HTTP
// trailer, grpc-status. net/http speaks HTTP/2 over TLS natively and supports
// trailers, so we stand up the whole thing ourselves and drive it end to end.
//
// The "capability-shaped" parts a real adapter owns — .proto compilation, full
// protobuf, HPACK/flow-control, deadlines, metadata — are out of scope here; the
// wire only ever sees CALL and CHANNEL, and that is exactly what this validates.
// The protobuf codec below handles one message: { string text = 1 }.
package grpc

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"net/http"
)

// --- minimal protobuf for message { string text = 1 } ---

// field 1, wire type 2 (length-delimited) => tag byte (1<<3)|2 = 0x0a
const strFieldTag = 0x0a

func encodeStringMsg(text string) []byte {
	out := []byte{strFieldTag}
	var v [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(v[:], uint64(len(text)))
	out = append(out, v[:n]...)
	return append(out, text...)
}

func decodeStringMsg(b []byte) (string, error) {
	if len(b) == 0 {
		return "", nil
	}
	if b[0] != strFieldTag {
		return "", fmt.Errorf("grpc: unexpected proto tag %#x", b[0])
	}
	n, m := binary.Uvarint(b[1:])
	if m <= 0 {
		return "", fmt.Errorf("grpc: bad length varint")
	}
	start := 1 + m
	if start+int(n) > len(b) {
		return "", fmt.Errorf("grpc: truncated string field")
	}
	return string(b[start : start+int(n)]), nil
}

// --- gRPC length-prefixed message framing ---

func writeFrame(w io.Writer, msg []byte) error {
	var hdr [5]byte
	hdr[0] = 0 // not compressed
	binary.BigEndian.PutUint32(hdr[1:], uint32(len(msg)))
	if _, err := w.Write(hdr[:]); err != nil {
		return err
	}
	_, err := w.Write(msg)
	return err
}

func readFrame(r io.Reader) ([]byte, error) {
	var hdr [5]byte
	if _, err := io.ReadFull(r, hdr[:]); err != nil {
		return nil, err // io.EOF here means "no more frames"
	}
	msg := make([]byte, binary.BigEndian.Uint32(hdr[1:]))
	if _, err := io.ReadFull(r, msg); err != nil {
		return nil, err
	}
	return msg, nil
}

// --- Server: two methods, one per mode ---

// StreamCount is how many frames the server-stream (CHANNEL) method emits.
const StreamCount = 3

// Handler is the gRPC service. /wire.Echo/Unary is CALL; /wire.Echo/Stream is
// CHANNEL. Both speak application/grpc and close with a grpc-status:0 trailer.
func Handler() http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc("/wire.Echo/Unary", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/grpc")
		w.Header().Set("Trailer", "grpc-status")
		req, err := readFrame(r.Body)
		if err != nil {
			w.Header().Set("grpc-status", "13") // INTERNAL
			return
		}
		text, _ := decodeStringMsg(req)
		_ = writeFrame(w, encodeStringMsg("echo:"+text))
		w.Header().Set("grpc-status", "0")
	})

	mux.HandleFunc("/wire.Echo/Stream", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/grpc")
		w.Header().Set("Trailer", "grpc-status")
		req, err := readFrame(r.Body)
		if err != nil {
			w.Header().Set("grpc-status", "13")
			return
		}
		text, _ := decodeStringMsg(req)
		fl, _ := w.(http.Flusher)
		for i := range StreamCount {
			_ = writeFrame(w, encodeStringMsg(fmt.Sprintf("%s#%d", text, i)))
			if fl != nil {
				fl.Flush() // push each frame as its own CHANNEL event
			}
		}
		w.Header().Set("grpc-status", "0")
	})

	return mux
}

// --- Client: drives CALL and CHANNEL over a caller-supplied HTTP/2 client ---

// Client speaks gRPC framing to base using hc (which must be HTTP/2-capable).
type Client struct {
	HC   *http.Client
	Base string
}

// Unary is the CALL: send one message, read exactly one back.
func (c *Client) Unary(text string) (string, error) {
	body := &bytes.Buffer{}
	if err := writeFrame(body, encodeStringMsg(text)); err != nil {
		return "", err
	}
	resp, err := c.HC.Post(c.Base+"/wire.Echo/Unary", "application/grpc", body)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	msg, err := readFrame(resp.Body)
	if err != nil {
		return "", err
	}
	return decodeStringMsg(msg)
}

// Stream is the CHANNEL: send one message, read frames until the stream ends.
func (c *Client) Stream(text string) ([]string, error) {
	body := &bytes.Buffer{}
	if err := writeFrame(body, encodeStringMsg(text)); err != nil {
		return nil, err
	}
	resp, err := c.HC.Post(c.Base+"/wire.Echo/Stream", "application/grpc", body)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	var out []string
	for {
		msg, err := readFrame(resp.Body)
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			break
		}
		if err != nil {
			return out, err
		}
		s, err := decodeStringMsg(msg)
		if err != nil {
			return out, err
		}
		out = append(out, s)
	}
	return out, nil
}
