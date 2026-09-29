// Package guard is the relay security gate shared by every relay adapter
// (gitbroker, mqtt): the structural checks that keep a credential from ever
// crossing a public artifact — a bridge repo, a broker topic — in either
// direction. Everything here is a pure function over the wire's httpx shapes;
// the adapters call it, they do not reimplement it.
//
// The four gates (four-system task #1145, G1–G4):
//
//   - Outside: a host-side file (policy, slots) must not live inside a
//     public working tree. A policy that ships with the bridge is a policy
//     the far end can rewrite; a slots file in the checkout is one `git add`
//     from public.
//   - CheckRequest: request headers are an ALLOWLIST (not a denylist of three
//     names), and the URL is checked structurally — no userinfo
//     (https://user:key@host), no credential-named query parameter
//     (?access_key=…), http(s) only.
//   - ResponseHeaders: a receipt echoes only Content-Type, Content-Length and
//     the witness's X-8-* reafference. Set-Cookie, Authorization echoes,
//     WWW-Authenticate and every other server header stay on the host.
//   - Egress: what a receipt may carry back. A slot-authenticated call
//     publishes only the body's sha256 unless the policy explicitly opts in
//     (authenticated_body) — a body fetched with the host's credential is
//     the host's, not the far end's, by default.
package guard

import (
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/rrrishi123/adapters/internal/httpx"
)

// --- G1/G2: host-side files must live outside a public working tree ---

// ErrInside is wrapped by Outside when path resolves inside dir.
var ErrInside = errors.New("guard: path is inside the public working tree")

// Outside returns the absolute form of path after refusing it when it
// resolves (symlinks included, for every prefix that exists) inside dir.
// An empty path is an error: the caller must name the file explicitly.
func Outside(path, dir string) (string, error) {
	if strings.TrimSpace(path) == "" {
		return "", errors.New("guard: path is required")
	}
	abs, err := filepath.Abs(path)
	if err != nil {
		return "", err
	}
	if strings.TrimSpace(dir) == "" {
		return abs, nil
	}
	dabs, err := filepath.Abs(dir)
	if err != nil {
		return "", err
	}
	if Inside(abs, dabs) {
		return "", fmt.Errorf("%w: %s is under %s — keep it outside the checkout", ErrInside, abs, dabs)
	}
	return abs, nil
}

// Inside reports whether path resolves to dir or somewhere beneath it.
// Symlinks are followed on the longest existing prefix of each, so a
// checkout reached through a symlink cannot hide the containment.
func Inside(path, dir string) bool {
	p := resolveExisting(path)
	d := resolveExisting(dir)
	rel, err := filepath.Rel(d, p)
	if err != nil {
		return false
	}
	if rel == "." {
		return true
	}
	return rel != ".." && !strings.HasPrefix(rel, ".."+string(filepath.Separator))
}

// resolveExisting evaluates symlinks on the longest prefix of p that exists
// and re-appends the rest, so a not-yet-created file still resolves under
// its real parent.
func resolveExisting(p string) string {
	p = filepath.Clean(p)
	var rest []string
	cur := p
	for {
		if r, err := filepath.EvalSymlinks(cur); err == nil {
			for i := len(rest) - 1; i >= 0; i-- {
				r = filepath.Join(r, rest[i])
			}
			return r
		}
		parent := filepath.Dir(cur)
		if parent == cur {
			return p
		}
		rest = append(rest, filepath.Base(cur))
		cur = parent
	}
}

// Exists reports whether path names a readable regular file.
func Exists(path string) bool {
	st, err := os.Stat(path)
	return err == nil && st.Mode().IsRegular()
}

// --- G4: request headers are an allowlist; the URL is checked structurally ---

// RequestHeaderAllow is the complete set of request header names an envelope
// may carry (case-insensitive). Anything else — Authorization, Cookie,
// X-Api-Key, X-Auth-Token, a header nobody has thought of yet — is refused.
// Credentials travel as an auth_slot NAME the host resolves, never inline.
var RequestHeaderAllow = map[string]bool{
	"accept":              true,
	"accept-encoding":     true,
	"accept-language":     true,
	"cache-control":       true,
	"content-type":        true,
	"if-match":            true,
	"if-modified-since":   true,
	"if-none-match":       true,
	"if-unmodified-since": true,
	"range":               true,
	"user-agent":          true,
	"x-correlation-id":    true,
	"x-request-id":        true,
}

// RequestHeaderAllowPrefix is the one allowed prefix: the wire's own
// reafference namespace (X-8-Actor etc.), which carries provenance, never a
// credential.
const RequestHeaderAllowPrefix = "x-8-"

// RequestHeaderAllowed reports whether a request header name may cross.
func RequestHeaderAllowed(name string) bool {
	n := strings.ToLower(strings.TrimSpace(name))
	return RequestHeaderAllow[n] || strings.HasPrefix(n, RequestHeaderAllowPrefix)
}

// CredentialQueryParams are query parameter names that structurally mean "a
// credential in the URL". A URL carrying any of them is refused regardless of
// value: a public artifact must not hold ?access_key=… any more than a header.
var CredentialQueryParams = map[string]bool{
	"access-key": true, "access_key": true, "accesskey": true,
	"access-token": true, "access_token": true, "accesstoken": true,
	"api-key": true, "api_key": true, "apikey": true,
	"auth": true, "auth-token": true, "auth_token": true, "authtoken": true, "authorization": true,
	"bearer": true, "client_secret": true, "client-secret": true,
	"credential": true, "credentials": true,
	"key": true, "passwd": true, "password": true, "pwd": true,
	"private_key": true, "private-key": true, "secret": true,
	"session": true, "session_id": true, "session-id": true, "sessionid": true, "sid": true,
	"sig": true, "signature": true, "token": true,
	"x-api-key": true, "x-auth-token": true,
}

// CheckURL refuses a URL that carries a credential structurally: userinfo
// (scheme://user:key@host), a credential-named query parameter, a non-http(s)
// scheme, or no host at all.
func CheckURL(raw string) error {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return errors.New("guard: url is required")
	}
	u, err := url.Parse(raw)
	if err != nil {
		return fmt.Errorf("guard: url does not parse: %v", err)
	}
	switch strings.ToLower(u.Scheme) {
	case "http", "https":
	default:
		return fmt.Errorf("guard: url scheme %q is not http or https", u.Scheme)
	}
	if u.Host == "" {
		return errors.New("guard: url has no host")
	}
	if u.User != nil {
		return errors.New("guard: url carries userinfo (credentials before the @ in the authority) — a public artifact must not hold them; name an auth_slot instead")
	}
	// the authority must not contain '@' at all, even if the parser folded it
	if i := strings.Index(raw, "//"); i >= 0 {
		auth := raw[i+2:]
		if j := strings.IndexAny(auth, "/?#"); j >= 0 {
			auth = auth[:j]
		}
		if strings.Contains(auth, "@") {
			return errors.New("guard: url carries userinfo (credentials before the @ in the authority) — a public artifact must not hold them; name an auth_slot instead")
		}
	}
	q, _ := url.ParseQuery(u.RawQuery) // partial on a bad escape is fine: we only need the names
	names := make([]string, 0, len(q))
	for k := range q {
		names = append(names, k)
	}
	sort.Strings(names)
	for _, k := range names {
		if CredentialQueryParams[strings.ToLower(strings.TrimSpace(k))] {
			return fmt.Errorf("guard: url query parameter %q names a credential — a public artifact must not carry it; name an auth_slot instead", k)
		}
	}
	return nil
}

// CheckRequest is the whole inbound gate on one proposed CALL: allowlisted
// request headers and a structurally credential-free URL.
func CheckRequest(r httpx.Request) error {
	if err := CheckURL(r.URL); err != nil {
		return err
	}
	names := make([]string, 0, len(r.Headers))
	for k := range r.Headers {
		names = append(names, k)
	}
	sort.Strings(names)
	for _, k := range names {
		if !RequestHeaderAllowed(k) {
			return fmt.Errorf("guard: request header %q is not in the allowlist (%s) — the relay artifact is public; name an auth_slot instead", k, AllowlistSummary())
		}
	}
	return nil
}

// AllowlistSummary renders RequestHeaderAllow for error messages.
func AllowlistSummary() string {
	names := make([]string, 0, len(RequestHeaderAllow)+1)
	for k := range RequestHeaderAllow {
		names = append(names, k)
	}
	sort.Strings(names)
	return strings.Join(names, ", ") + ", " + RequestHeaderAllowPrefix + "*"
}

// --- G3: response headers are an allowlist too ---

// ResponseHeaderAllow is the set of response header names a receipt may echo;
// ResponseHeaderAllowPrefix adds the witness's reafference (X-8-*).
var ResponseHeaderAllow = map[string]bool{
	"content-type":   true,
	"content-length": true,
}

// ResponseHeaderAllowPrefix is the witness namespace: X-8-Witness,
// X-8-Ledger-Id, X-8-Ledger, X-8-DMs — proof, never a secret.
const ResponseHeaderAllowPrefix = "x-8-"

// ResponseHeaderAllowed reports whether a response header may enter a receipt.
func ResponseHeaderAllowed(name string) bool {
	n := strings.ToLower(strings.TrimSpace(name))
	return ResponseHeaderAllow[n] || strings.HasPrefix(n, ResponseHeaderAllowPrefix)
}

// ResponseHeaders filters h down to the allowlist, first value each, keys in
// canonical form. Set-Cookie and friends never come out. Returns nil when
// nothing survives so the receipt omits the field entirely.
func ResponseHeaders(h http.Header) map[string]string {
	if len(h) == 0 {
		return nil
	}
	out := map[string]string{}
	for k, v := range h {
		if len(v) == 0 || !ResponseHeaderAllowed(k) {
			continue
		}
		out[http.CanonicalHeaderKey(k)] = v[0]
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

// --- G3: egress — what a receipt may carry back ---

// Egress is the outbound gate a policy attaches to its decision. The zero
// value is the SAFE default for a relay whose artifact is public: nothing.
// Use Default() for the loopback-friendly default (capped body for
// unauthenticated calls, allowlisted headers, digest-only for slot calls).
type Egress struct {
	Body    bool `json:"body"`    // include the (capped) response body — for calls WITHOUT an auth_slot
	Headers bool `json:"headers"` // include the ALLOWLISTED response headers (Content-Type, Content-Length, X-8-*)
	// AuthenticatedBody is the explicit opt-in for publishing the body of a
	// slot-authenticated call. Absent/false: only body_len + body_digest.
	AuthenticatedBody bool `json:"authenticated_body,omitempty"`
}

// DefaultEgress is what applies when a policy does not say: body for plain
// calls, allowlisted headers, digest-only for authenticated calls.
func DefaultEgress() *Egress { return &Egress{Body: true, Headers: true} }

// PublishBody reports whether the response body may enter the receipt for a
// call that was (or was not) fired with a host credential.
func (e *Egress) PublishBody(authenticated bool) bool {
	if e == nil {
		e = DefaultEgress()
	}
	if authenticated {
		return e.AuthenticatedBody
	}
	return e.Body
}

// PublishHeaders reports whether the allowlisted response headers may enter
// the receipt.
func (e *Egress) PublishHeaders() bool {
	if e == nil {
		e = DefaultEgress()
	}
	return e.Headers
}
