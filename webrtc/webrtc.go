// Package webrtc is the WebRTC adapter's own-both-ends conformance harness for
// the ONE part of WebRTC the wire actually touches: the signaling CALL.
//
// Per transports.json: "SDP/ICE/DTLS negotiation is capability-shaped (signaled
// over an httpx CALL); the resulting DataChannel/media is a wsx-style CHANNEL.
// The wire never sees SDP." So the wire's role in WebRTC is a single CALL that
// carries an SDP offer to a peer and returns an SDP answer. That is what this
// harness stands up and drives, both ends ours.
//
// What is deliberately NOT here: DTLS, ICE, SCTP, the DataChannel bytes. Those
// require a full WebRTC stack (e.g. pion) and are the adapter's job — the wire
// never sees them. This is a boundary, not a shortcut: we validate the exact
// surface the wire is responsible for (offer in -> answer out over a CALL) and
// leave the crypto interior to the layer that owns it.
package webrtc

import (
	"fmt"
	"io"
	"net/http"
	"strings"
)

// SDP is a minimal WebRTC session description for a single DataChannel m-line.
// It is realistic enough to prove a genuine offer/answer negotiation (media,
// mid, DTLS role flip, and a fingerprint that ties the future DataChannel to a
// crypto identity) without pulling in a DTLS/SCTP stack.
type SDP struct {
	SessionID   string
	Setup       string // "actpass" in an offer, "active" (or "passive") in an answer
	Mid         string
	IceUfrag    string
	IcePwd      string
	Fingerprint string // sha-256 DTLS fingerprint
}

// Marshal renders the SDP to the on-the-wire text an httpx CALL would carry.
func (s SDP) Marshal() string {
	return strings.Join([]string{
		"v=0",
		"o=- " + s.SessionID + " 2 IN IP4 127.0.0.1",
		"s=-",
		"t=0 0",
		"a=group:BUNDLE " + s.Mid,
		"m=application 9 UDP/DTLS/SCTP webrtc-datachannel",
		"c=IN IP4 0.0.0.0",
		"a=ice-ufrag:" + s.IceUfrag,
		"a=ice-pwd:" + s.IcePwd,
		"a=fingerprint:sha-256 " + s.Fingerprint,
		"a=setup:" + s.Setup,
		"a=mid:" + s.Mid,
		"a=sctp-port:5000",
		"",
	}, "\r\n")
}

// ParseSDP pulls the fields we negotiate on out of an SDP body.
func ParseSDP(body string) (SDP, error) {
	var s SDP
	hasDataChannel := false
	for line := range strings.SplitSeq(body, "\n") {
		line = strings.TrimRight(line, "\r")
		switch {
		case strings.HasPrefix(line, "m=application") && strings.Contains(line, "webrtc-datachannel"):
			hasDataChannel = true
		case strings.HasPrefix(line, "a=setup:"):
			s.Setup = strings.TrimPrefix(line, "a=setup:")
		case strings.HasPrefix(line, "a=mid:"):
			s.Mid = strings.TrimPrefix(line, "a=mid:")
		case strings.HasPrefix(line, "a=ice-ufrag:"):
			s.IceUfrag = strings.TrimPrefix(line, "a=ice-ufrag:")
		case strings.HasPrefix(line, "a=ice-pwd:"):
			s.IcePwd = strings.TrimPrefix(line, "a=ice-pwd:")
		case strings.HasPrefix(line, "a=fingerprint:sha-256 "):
			s.Fingerprint = strings.TrimPrefix(line, "a=fingerprint:sha-256 ")
		}
	}
	if !hasDataChannel {
		return s, fmt.Errorf("webrtc: offer has no webrtc-datachannel m-line")
	}
	if s.Mid == "" || s.Setup == "" || s.Fingerprint == "" {
		return s, fmt.Errorf("webrtc: offer missing mid/setup/fingerprint")
	}
	return s, nil
}

// Answer produces a valid SDP answer for an offer: it keeps the media and mid,
// flips the DTLS role (actpass -> active), and presents its own ICE creds and
// fingerprint. This is the real negotiation the CALL carries.
func Answer(offer SDP, myFingerprint string) SDP {
	role := "active"
	if offer.Setup == "active" {
		role = "passive"
	}
	return SDP{
		SessionID:   offer.SessionID + "1",
		Setup:       role,
		Mid:         offer.Mid,
		IceUfrag:    "ans" + offer.IceUfrag,
		IcePwd:      "answerpwd" + offer.Mid,
		Fingerprint: myFingerprint,
	}
}

// SignalingHandler is the answering peer's signaling endpoint: it receives an
// SDP offer over an http CALL and returns an SDP answer. This is the exact
// surface the wire's http_request atom drives for WebRTC.
func SignalingHandler(myFingerprint string) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, "read: "+err.Error(), http.StatusBadRequest)
			return
		}
		offer, err := ParseSDP(string(body))
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		w.Header().Set("Content-Type", "application/sdp")
		_, _ = io.WriteString(w, Answer(offer, myFingerprint).Marshal())
	})
}
