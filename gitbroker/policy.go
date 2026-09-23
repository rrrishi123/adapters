package gitbroker

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"

	"github.com/rrrishi123/adapters/internal/guard"
)

// Policy is the authoritative side's hook: the far end proposes, this decides.
// A Policy sees the validated envelope and returns a Decision; Fired=false
// means the poller writes a refusal receipt and never fires.
//
// HALT — the far end's revocation — is not a Policy: it is the state/halt
// file in the bridge, checked by the Poller immediately before EVERY fire
// (Config.HaltFile). A halted poll stops without answering, so the envelopes
// fire once the halt is lifted.
type Policy interface {
	Decide(env *Envelope) Decision
}

// PolicyFunc adapts a function to Policy.
type PolicyFunc func(env *Envelope) Decision

// Decide implements Policy.
func (f PolicyFunc) Decide(env *Envelope) Decision { return f(env) }

// AllowAll fires everything that validated with the default egress. It is
// NOT a default anywhere: a Poller with no Policy is Closed. Loopbacks and
// tests set it explicitly, in code, where it is visible.
var AllowAll Policy = PolicyFunc(func(*Envelope) Decision {
	return Decision{Fired: true, Policy: "allow-all", Egress: guard.DefaultEgress()}
})

// Closed refuses everything. It is what a Poller runs with when no Policy was
// configured — the relay fails shut, never open.
var Closed Policy = PolicyFunc(func(*Envelope) Decision {
	return Decision{Fired: false, Policy: "closed", Reason: "no policy configured — closed"}
})

// Egress is the outbound gate a policy attaches to its decision (guard.Egress).
type Egress = guard.Egress

// FilePolicy is the reference policy: a JSON file OUTSIDE the bridge checkout
// (the bridge is public; a policy inside it is a policy the far end can
// rewrite), re-read on every decision so the operator can tighten it without
// restarting the poller.
//
//	{
//	  "allow_methods":      ["GET", "POST"],                 // empty = any method
//	  "allow_url_prefixes": ["http://127.0.0.1:7070/"],     // empty = any URL
//	  "egress":             {"body": true, "headers": true, "authenticated_body": false}
//	}
//
// A missing, unreadable or malformed file means CLOSED (deny): a policy that
// is not there is not a policy, and a broken one must fail safe. Use
// NewFilePolicy to also refuse a path inside the bridge.
type FilePolicy struct {
	Path string
}

// NewFilePolicy builds a FilePolicy after refusing a path that resolves inside
// bridgeDir (symlinks followed). It does NOT require the file to exist yet —
// a missing file is simply closed at decision time.
func NewFilePolicy(path, bridgeDir string) (FilePolicy, error) {
	abs, err := guard.Outside(path, bridgeDir)
	if err != nil {
		return FilePolicy{}, fmt.Errorf("gitbroker: policy: %w", err)
	}
	return FilePolicy{Path: abs}, nil
}

type filePolicyDoc struct {
	AllowMethods     []string `json:"allow_methods"`
	AllowURLPrefixes []string `json:"allow_url_prefixes"`
	Egress           *Egress  `json:"egress"`
}

// Decide implements Policy.
func (p FilePolicy) Decide(env *Envelope) Decision {
	const name = "file-policy"
	if strings.TrimSpace(p.Path) == "" {
		return Decision{Fired: false, Policy: name, Reason: "no policy path — closed"}
	}
	b, err := os.ReadFile(p.Path)
	if errors.Is(err, os.ErrNotExist) {
		return Decision{Fired: false, Policy: name, Reason: "policy file missing — closed"}
	}
	if err != nil {
		return Decision{Fired: false, Policy: name, Reason: "policy unreadable: " + err.Error()}
	}
	var doc filePolicyDoc
	if err := json.Unmarshal(b, &doc); err != nil {
		return Decision{Fired: false, Policy: name, Reason: "policy malformed: " + err.Error()}
	}
	if len(doc.AllowMethods) > 0 && !containsFold(doc.AllowMethods, env.Call.Method) {
		return Decision{Fired: false, Policy: name, Reason: "method " + env.Call.Method + " not in allow_methods"}
	}
	if len(doc.AllowURLPrefixes) > 0 {
		ok := false
		for _, pre := range doc.AllowURLPrefixes {
			if strings.HasPrefix(env.Call.URL, pre) {
				ok = true
				break
			}
		}
		if !ok {
			return Decision{Fired: false, Policy: name, Reason: "url not under any allow_url_prefixes"}
		}
	}
	return Decision{Fired: true, Policy: name, Egress: doc.Egress}
}

func containsFold(xs []string, s string) bool {
	for _, x := range xs {
		if strings.EqualFold(x, s) {
			return true
		}
	}
	return false
}
