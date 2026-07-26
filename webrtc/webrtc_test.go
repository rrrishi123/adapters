package webrtc

// Own-both-ends validation of the WebRTC signaling CALL — the only part of
// WebRTC the wire touches. An offerer POSTs an SDP offer to the answering peer's
// signaling endpoint (an http CALL) and gets back an SDP answer; we assert the
// answer is a coherent negotiation of the offer. The DataChannel's DTLS/SCTP
// interior is intentionally NOT here (adapter/pion territory the wire never sees).

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestWebRTC_SignalingCall_OfferAnswer(t *testing.T) {
	// The answering peer stands up its signaling endpoint (we own it).
	const answererFP = "AA:BB:CC:DD:EE:FF:00:11:22:33:44:55:66:77:88:99"
	srv := httptest.NewServer(SignalingHandler(answererFP))
	defer srv.Close()

	// The offerer builds an offer and fires it as a CALL.
	offer := SDP{
		SessionID:   "42",
		Setup:       "actpass",
		Mid:         "0",
		IceUfrag:    "off1",
		IcePwd:      "offererpassword",
		Fingerprint: "01:23:45:67:89:AB:CD:EF:01:23:45:67:89:AB:CD:EF",
	}
	resp, err := http.Post(srv.URL+"/signal", "application/sdp", strings.NewReader(offer.Marshal()))
	if err != nil {
		t.Fatalf("signaling CALL: %v", err)
	}
	defer resp.Body.Close()
	if ct := resp.Header.Get("Content-Type"); ct != "application/sdp" {
		t.Fatalf("answer content-type = %q, want application/sdp", ct)
	}
	raw, _ := io.ReadAll(resp.Body)

	answer, err := ParseSDP(string(raw))
	if err != nil {
		t.Fatalf("answer does not parse as SDP: %v", err)
	}

	// A coherent negotiation: same media+mid, DTLS role flipped, answerer's own
	// fingerprint present (the crypto identity the DataChannel will bind to).
	if answer.Mid != offer.Mid {
		t.Fatalf("answer mid = %q, want %q (must match offer)", answer.Mid, offer.Mid)
	}
	if answer.Setup != "active" {
		t.Fatalf("answer setup = %q, want active (must flip from offer's actpass)", answer.Setup)
	}
	if answer.Fingerprint != answererFP {
		t.Fatalf("answer fingerprint = %q, want the answerer's %q", answer.Fingerprint, answererFP)
	}
	if answer.IceUfrag == offer.IceUfrag {
		t.Fatal("answer reused the offerer's ICE ufrag; each peer must present its own")
	}
}

// A malformed offer (no datachannel m-line) must be rejected — the signaling
// CALL validates what it carries rather than echoing anything back.
func TestWebRTC_SignalingCall_RejectsNonDataChannel(t *testing.T) {
	srv := httptest.NewServer(SignalingHandler("FP"))
	defer srv.Close()

	resp, err := http.Post(srv.URL+"/signal", "application/sdp",
		strings.NewReader("v=0\r\nm=audio 9 UDP/TLS/RTP/SAVPF 111\r\n"))
	if err != nil {
		t.Fatalf("call: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("non-datachannel offer accepted with status %d, want 400", resp.StatusCode)
	}
}
