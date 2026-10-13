// Package webrtc owns SDP/ICE/DTLS/SCTP negotiation and exposes the resulting
// DataChannel as a wsx-style CHANNEL. Signaling is an adapter-owned httpx CALL;
// callers exchange command/event text and never need to handle SDP.
package webrtc

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/pion/ice/v4"
	pion "github.com/pion/webrtc/v4"
	"github.com/rrrishi123/adapters/internal/httpx"
)

const maxMessageSize = 64 * 1024
const maxSDPSize = 1024 * 1024

// ICEServer allows each node to configure STUN/TURN URLs and TURN credentials.
type ICEServer = pion.ICEServer

// Config has no default deployment endpoints. Empty ICEServers means host
// candidates only. LoopbackOnly restricts local and remote candidates to
// localhost for an offline own-both-ends test.
type Config struct {
	SignalingURL string
	ICEServers   []ICEServer
	Label        string
	Timeout      time.Duration
	LoopbackOnly bool
	HTTPClient   *http.Client
}

func (c Config) normalized() (Config, error) {
	if c.Label == "" {
		c.Label = "wire"
	}
	if c.Timeout == 0 {
		c.Timeout = 15 * time.Second
	}
	if c.Timeout < 0 {
		return c, errors.New("webrtc: timeout must be positive")
	}
	if c.LoopbackOnly && len(c.ICEServers) != 0 {
		return c, errors.New("webrtc: loopback-only cannot use ICE servers")
	}
	return c, nil
}

func newPeer(c Config) (*pion.PeerConnection, error) {
	var settings pion.SettingEngine
	settings.DetachDataChannels()
	settings.EnableDataChannelBlockWrite(true)
	settings.SetSCTPMaxMessageSize(maxMessageSize)
	if c.LoopbackOnly {
		settings.SetNetworkTypes([]pion.NetworkType{pion.NetworkTypeUDP4})
		settings.SetIncludeLoopbackCandidate(true)
		settings.SetIPFilter(func(ip net.IP) bool { return ip.IsLoopback() })
		settings.SetRemoteIPFilter(func(ip net.IP) bool { return ip.IsLoopback() })
		settings.SetICEMulticastDNSMode(ice.MulticastDNSModeDisabled)
	}
	return pion.NewAPI(pion.WithSettingEngine(settings)).NewPeerConnection(pion.Configuration{ICEServers: c.ICEServers})
}

// A detached DataChannel preserves message boundaries and text/binary types,
// while providing SCTP read/write deadlines and backpressure.
type dataChannel interface {
	ReadDataChannel([]byte) (int, bool, error)
	WriteDataChannel([]byte, bool) (int, error)
	SetReadDeadline(time.Time) error
	SetWriteDeadline(time.Time) error
	Close() error
}

// Conn has the wsx Conn surface: one reader, concurrent serialized writers,
// deadlines and Close. One connection carries many commands and peer events.
type Conn struct {
	pc        *pion.PeerConnection
	mu        sync.Mutex
	raw       dataChannel
	ready     chan struct{}
	done      chan struct{}
	closeOnce sync.Once
	closeErr  error
	wmu       sync.Mutex
}

func newConn(pc *pion.PeerConnection) *Conn {
	c := &Conn{pc: pc, ready: make(chan struct{}), done: make(chan struct{})}
	pc.OnConnectionStateChange(func(state pion.PeerConnectionState) {
		if state == pion.PeerConnectionStateFailed || state == pion.PeerConnectionStateClosed {
			// Pion callbacks must not synchronously close their own transport.
			go c.Close()
		}
	})
	return c
}

func (c *Conn) bind(dc *pion.DataChannel) {
	dc.OnOpen(func() {
		raw, err := dc.DetachWithDeadline()
		if err != nil {
			go c.Close()
			return
		}
		c.mu.Lock()
		defer c.mu.Unlock()
		select {
		case <-c.done:
			_ = raw.Close()
		default:
			c.raw = raw
			close(c.ready)
		}
	})
}

func (c *Conn) waitReady(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-c.done:
		return io.ErrClosedPipe
	case <-c.ready:
		return nil
	}
}

func (c *Conn) channel() (dataChannel, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	select {
	case <-c.done:
		return nil, io.ErrClosedPipe
	default:
	}
	if c.raw == nil {
		return nil, errors.New("webrtc: DataChannel is not open")
	}
	return c.raw, nil
}

// WriteText sends exactly one reliable, ordered DataChannel text message.
func (c *Conn) WriteText(text string) error {
	if len(text) > maxMessageSize {
		return errors.New("webrtc: text exceeds 64 KiB message limit")
	}
	c.wmu.Lock()
	defer c.wmu.Unlock()
	raw, err := c.channel()
	if err != nil {
		return err
	}
	n, err := raw.WriteDataChannel([]byte(text), true)
	if err == nil && n != len(text) {
		return io.ErrShortWrite
	}
	return err
}

// ReadText returns the next text message, skipping binary messages like wsx.
func (c *Conn) ReadText() (string, error) {
	raw, err := c.channel()
	if err != nil {
		return "", err
	}
	buf := make([]byte, maxMessageSize)
	for {
		n, text, err := raw.ReadDataChannel(buf)
		if err != nil {
			return "", err
		}
		if text {
			return string(buf[:n]), nil
		}
	}
}

// SetDeadline bounds future reads and writes, including blocked operations.
func (c *Conn) SetDeadline(t time.Time) error {
	raw, err := c.channel()
	if err != nil {
		return err
	}
	return errors.Join(raw.SetReadDeadline(t), raw.SetWriteDeadline(t))
}

// Close tears down the peer connection and unblocks pending IO.
func (c *Conn) Close() error {
	c.closeOnce.Do(func() {
		c.mu.Lock()
		close(c.done)
		c.mu.Unlock()
		c.closeErr = c.pc.Close()
	})
	return c.closeErr
}

// BidiCommand sends the {id,method,params} shape of bidi_command and returns
// the matching response. It owns the reader during the exchange; unmatched
// frames (including events) go to onEvent when supplied. SetDeadline bounds it.
func (c *Conn) BidiCommand(id int, method string, params map[string]any, onEvent func(string)) (json.RawMessage, error) {
	if method == "" {
		return nil, errors.New("webrtc: command method is required")
	}
	if params == nil {
		params = map[string]any{}
	}
	command, err := json.Marshal(struct {
		ID     int            `json:"id"`
		Method string         `json:"method"`
		Params map[string]any `json:"params"`
	}{id, method, params})
	if err != nil {
		return nil, err
	}
	if err := c.WriteText(string(command)); err != nil {
		return nil, err
	}
	for {
		frame, err := c.ReadText()
		if err != nil {
			return nil, err
		}
		var response struct {
			ID *int `json:"id"`
		}
		if json.Unmarshal([]byte(frame), &response) == nil && response.ID != nil && *response.ID == id {
			return json.RawMessage(frame), nil
		}
		if onEvent != nil {
			onEvent(frame)
		}
	}
}

// contextTransport preserves httpx.Do's CALL contract while bounding the
// signaling exchange with the encompassing negotiation context.
type contextTransport struct {
	ctx  context.Context
	base http.RoundTripper
}

func (t contextTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	resp, err := t.base.RoundTrip(r.Clone(t.ctx))
	if err == nil {
		resp.Body = &limitedBody{Reader: io.LimitReader(resp.Body, maxSDPSize+1), Closer: resp.Body}
	}
	return resp, err
}

type limitedBody struct {
	io.Reader
	io.Closer
}

// Dial gathers ICE candidates, exchanges one non-trickle SDP offer/answer via
// httpx, then waits for ICE + DTLS + SCTP + DCEP to open the DataChannel.
// ctx bounds negotiation only; the returned connection lives until Close.
func Dial(ctx context.Context, cfg Config) (_ *Conn, err error) {
	cfg, err = cfg.normalized()
	if err != nil {
		return nil, err
	}
	u, err := url.Parse(cfg.SignalingURL)
	if err != nil || u.Host == "" || (u.Scheme != "http" && u.Scheme != "https") {
		return nil, errors.New("webrtc: signaling URL must be an explicit http(s) endpoint")
	}
	ctx, cancel := context.WithTimeout(ctx, cfg.Timeout)
	defer cancel()
	pc, err := newPeer(cfg)
	if err != nil {
		return nil, err
	}
	c := newConn(pc)
	defer func() {
		if err != nil {
			_ = c.Close()
		}
	}()
	dc, err := pc.CreateDataChannel(cfg.Label, nil)
	if err != nil {
		return nil, err
	}
	c.bind(dc)
	offer, err := pc.CreateOffer(nil)
	if err != nil {
		return nil, err
	}
	if err = gather(ctx, pc, offer); err != nil {
		return nil, err
	}
	var client http.Client
	if cfg.HTTPClient != nil {
		client = *cfg.HTTPClient
	}
	base := client.Transport
	if base == nil {
		base = http.DefaultTransport
	}
	client.Transport = contextTransport{ctx: ctx, base: base}
	// Do not forward an offer (and ICE credentials) to a redirect target.
	client.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	resp, err := httpx.Do(&client, httpx.Request{Method: http.MethodPost, URL: cfg.SignalingURL,
		Headers: map[string]string{"Content-Type": "application/sdp", "Accept": "application/sdp"}, Body: pc.LocalDescription().SDP})
	if err != nil {
		return nil, fmt.Errorf("webrtc: signaling CALL: %w", err)
	}
	if resp.Status != http.StatusOK || len(resp.Body) > maxSDPSize {
		return nil, fmt.Errorf("webrtc: signaling CALL refused (status %d or oversized answer)", resp.Status)
	}
	if err = pc.SetRemoteDescription(pion.SessionDescription{Type: pion.SDPTypeAnswer, SDP: resp.Body}); err != nil {
		return nil, fmt.Errorf("webrtc: invalid answer: %w", err)
	}
	if err = c.waitReady(ctx); err != nil {
		return nil, fmt.Errorf("webrtc: opening DataChannel: %w", err)
	}
	return c, nil
}

func gather(ctx context.Context, pc *pion.PeerConnection, desc pion.SessionDescription) error {
	complete := pion.GatheringCompletePromise(pc)
	if err := pc.SetLocalDescription(desc); err != nil {
		return err
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-complete:
		return nil
	}
}

// Signaler is the answering node's HTTP handler. Each successful POST opens
// one CHANNEL. The callback runs once it is open, owning it until returning.
// Close cancels handshakes and closes all live channels.
type Signaler struct {
	cfg    Config
	accept func(*Conn)
	ctx    context.Context
	cancel context.CancelFunc
	mu     sync.Mutex
	peers  map[*Conn]struct{}
}

func NewSignaler(cfg Config, accept func(*Conn)) (*Signaler, error) {
	cfg, err := cfg.normalized()
	if err != nil {
		return nil, err
	}
	if accept == nil {
		return nil, errors.New("webrtc: channel handler is required")
	}
	ctx, cancel := context.WithCancel(context.Background())
	return &Signaler{cfg: cfg, accept: accept, ctx: ctx, cancel: cancel, peers: make(map[*Conn]struct{})}, nil
}

func (s *Signaler) Close() error {
	s.mu.Lock()
	s.cancel()
	peers := make([]*Conn, 0, len(s.peers))
	for c := range s.peers {
		peers = append(peers, c)
	}
	s.mu.Unlock()
	var errs []error
	for _, c := range peers {
		errs = append(errs, c.Close())
	}
	return errors.Join(errs...)
}

func (s *Signaler) release(c *Conn) {
	_ = c.Close()
	s.mu.Lock()
	delete(s.peers, c)
	s.mu.Unlock()
}

func (s *Signaler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		http.Error(w, "POST an SDP offer", http.StatusMethodNotAllowed)
		return
	}
	if strings.TrimSpace(strings.Split(r.Header.Get("Content-Type"), ";")[0]) != "application/sdp" {
		http.Error(w, "expected application/sdp", http.StatusUnsupportedMediaType)
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, maxSDPSize)
	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "invalid or oversized offer", http.StatusBadRequest)
		return
	}
	if !strings.Contains(string(body), "webrtc-datachannel") {
		http.Error(w, "offer must negotiate a DataChannel", http.StatusBadRequest)
		return
	}
	pc, err := newPeer(s.cfg)
	if err != nil {
		http.Error(w, "cannot create peer", http.StatusInternalServerError)
		return
	}
	c := newConn(pc)
	s.mu.Lock()
	if s.ctx.Err() != nil {
		s.mu.Unlock()
		_ = c.Close()
		http.Error(w, "signaler closed", http.StatusServiceUnavailable)
		return
	}
	s.peers[c] = struct{}{}
	s.mu.Unlock()
	handedOff := false
	defer func() {
		if !handedOff {
			s.release(c)
		}
	}()
	var bindOnce sync.Once
	pc.OnDataChannel(func(dc *pion.DataChannel) {
		bound := false
		if dc.Label() == s.cfg.Label && dc.Ordered() && dc.MaxRetransmits() == nil && dc.MaxPacketLifeTime() == nil {
			bindOnce.Do(func() { c.bind(dc); bound = true })
		}
		if !bound {
			go dc.Close()
		}
	})
	if err := pc.SetRemoteDescription(pion.SessionDescription{Type: pion.SDPTypeOffer, SDP: string(body)}); err != nil {
		http.Error(w, "invalid DataChannel offer", http.StatusBadRequest)
		return
	}
	answer, err := pc.CreateAnswer(nil)
	if err != nil {
		http.Error(w, "cannot answer offer", http.StatusBadRequest)
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), s.cfg.Timeout)
	defer cancel()
	stop := context.AfterFunc(s.ctx, cancel)
	defer stop()
	if err := gather(ctx, pc, answer); err != nil {
		http.Error(w, "ICE gathering failed", http.StatusGatewayTimeout)
		return
	}
	w.Header().Set("Content-Type", "application/sdp")
	w.Header().Set("Cache-Control", "no-store")
	if _, err := io.WriteString(w, pc.LocalDescription().SDP); err != nil {
		return
	}
	handedOff = true
	go func() {
		defer s.release(c)
		// HTTP ends before ICE/DTLS completes. CHANNEL lifetime belongs to the
		// signaler, never the completed signaling request's context.
		ctx, cancel := context.WithTimeout(s.ctx, s.cfg.Timeout)
		defer cancel()
		if c.waitReady(ctx) == nil {
			s.accept(c)
		}
	}()
}
