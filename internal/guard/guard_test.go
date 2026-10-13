package guard

import (
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/rrrishi123/adapters/internal/httpx"
)

// G1/G2: a policy or slots file inside the public checkout is refused, even
// through a symlink; outside is fine; empty is an error (must be explicit).
func TestGuard_Outside(t *testing.T) {
	root := t.TempDir()
	bridge := filepath.Join(root, "bridge")
	os.MkdirAll(filepath.Join(bridge, "state"), 0o755)
	os.WriteFile(filepath.Join(bridge, "state", "policy.json"), []byte("{}"), 0o644)
	if _, err := Outside(filepath.Join(bridge, "state", "policy.json"), bridge); err == nil {
		t.Fatal("policy inside the bridge must be refused")
	}
	if _, err := Outside(filepath.Join(bridge, "not-yet-created.json"), bridge); err == nil {
		t.Fatal("a not-yet-created file inside the bridge must be refused")
	}
	if _, err := Outside(bridge, bridge); err == nil {
		t.Fatal("the bridge dir itself is inside")
	}
	// a sibling whose name merely starts with the bridge name is outside
	if _, err := Outside(filepath.Join(root, "bridge-policy.json"), bridge); err != nil {
		t.Fatalf("sibling with a shared prefix is outside: %v", err)
	}
	if abs, err := Outside(filepath.Join(root, "policy.json"), bridge); err != nil || !filepath.IsAbs(abs) {
		t.Fatalf("outside: %v %q", err, abs)
	}
	if _, err := Outside("", bridge); err == nil {
		t.Fatal("empty path must be an error: the operator names the file explicitly")
	}
	// symlink: a link outside pointing into the bridge is still inside
	link := filepath.Join(root, "policy-link.json")
	if err := os.Symlink(filepath.Join(bridge, "state", "policy.json"), link); err == nil {
		if _, err := Outside(link, bridge); err == nil {
			t.Fatal("a symlink into the bridge must be refused")
		}
	}
	// symlink: the bridge reached through a link
	blink := filepath.Join(root, "bridge-link")
	if err := os.Symlink(bridge, blink); err == nil {
		if _, err := Outside(filepath.Join(blink, "state", "policy.json"), bridge); err == nil {
			t.Fatal("a path through a symlinked bridge must be refused")
		}
	}
}

// G4: request headers are an allowlist; every credential-shaped header and
// every credential-shaped URL is refused.
func TestGuard_CheckRequest(t *testing.T) {
	ok := []httpx.Request{
		{URL: "http://127.0.0.1:7070/status"},
		{URL: "https://example.test/x?q=hello&page=2", Headers: map[string]string{"Accept": "application/json", "User-Agent": "far-end", "X-8-Actor": "sandbox"}},
		{URL: "http://h/p", Headers: map[string]string{"content-type": "text/plain", "If-None-Match": "abc"}},
	}
	for _, r := range ok {
		if err := CheckRequest(r); err != nil {
			t.Errorf("%+v should pass: %v", r, err)
		}
	}
	bad := map[string]httpx.Request{
		"authorization":       {URL: "http://h/", Headers: map[string]string{"Authorization": "Bearer x"}},
		"proxy-authorization": {URL: "http://h/", Headers: map[string]string{"Proxy-Authorization": "Basic x"}},
		"cookie":              {URL: "http://h/", Headers: map[string]string{"Cookie": "sid=1"}},
		"x-api-key":           {URL: "http://h/", Headers: map[string]string{"X-Api-Key": "k"}},
		"x-auth-token":        {URL: "http://h/", Headers: map[string]string{"x-auth-token": "k"}},
		"unknown header":      {URL: "http://h/", Headers: map[string]string{"X-Secret-Thing": "k"}},
		"userinfo":            {URL: "https://user:key@host/path"},
		"userinfo no pw":      {URL: "https://token@host/path"},
		"access_key":          {URL: "https://host/path?access_key=abc"},
		"api_key upper":       {URL: "https://host/path?API_KEY=abc"},
		"token":               {URL: "https://host/path?a=1&token=abc"},
		"sig":                 {URL: "https://host/blob?sv=1&sig=abc"},
		"scheme file":         {URL: "file:///etc/passwd"},
		"scheme ftp":          {URL: "ftp://h/x"},
		"no host":             {URL: "http:///x"},
		"empty":               {URL: ""},
	}
	for name, r := range bad {
		if err := CheckRequest(r); err == nil {
			t.Errorf("%s: %+v must be refused", name, r)
		}
	}
}

// G3: a receipt echoes only Content-Type, Content-Length and X-8-*; a
// Set-Cookie (or any other server header) never comes out.
func TestGuard_ResponseHeaders(t *testing.T) {
	h := http.Header{}
	h.Set("Content-Type", "application/json")
	h.Set("Content-Length", "12")
	h.Set("X-8-Witness", "seen · act #1")
	h.Set("X-8-Ledger-Id", "1")
	h.Add("Set-Cookie", "session=secret; HttpOnly")
	h.Set("WWW-Authenticate", "Bearer realm=x")
	h.Set("Authorization", "Bearer echoed")
	h.Set("Server", "nginx")
	h.Set("X-Powered-By", "php")
	out := ResponseHeaders(h)
	for _, k := range []string{"Set-Cookie", "WWW-Authenticate", "Authorization", "Server", "X-Powered-By"} {
		if _, leaked := out[k]; leaked {
			t.Errorf("%s leaked into the receipt: %v", k, out)
		}
	}
	for _, k := range []string{"Content-Type", "Content-Length", "X-8-Witness", "X-8-Ledger-Id"} {
		if _, ok := out[k]; !ok {
			t.Errorf("%s missing from the receipt: %v", k, out)
		}
	}
	if len(out) != 4 {
		t.Fatalf("want exactly 4 allowlisted headers, got %v", out)
	}
	if ResponseHeaders(nil) != nil || ResponseHeaders(http.Header{"Set-Cookie": {"x"}}) != nil {
		t.Fatal("nothing surviving must be nil so the field is omitted")
	}
}

// G3: egress — an authenticated call publishes the body only on explicit opt-in.
func TestGuard_Egress(t *testing.T) {
	var nilE *Egress
	if !nilE.PublishBody(false) || nilE.PublishBody(true) || !nilE.PublishHeaders() {
		t.Fatal("nil egress must default to: body for plain calls, digest-only for slot calls, headers on")
	}
	e := &Egress{Body: true, Headers: true}
	if e.PublishBody(true) {
		t.Fatal("slot-authenticated body needs authenticated_body:true")
	}
	e.AuthenticatedBody = true
	if !e.PublishBody(true) {
		t.Fatal("explicit opt-in must publish")
	}
	zero := &Egress{}
	if zero.PublishBody(false) || zero.PublishHeaders() {
		t.Fatal("the zero egress is nothing")
	}
	if !strings.Contains(AllowlistSummary(), "x-8-*") {
		t.Fatal(AllowlistSummary())
	}
}
