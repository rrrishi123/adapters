package gitbroker

import (
	"encoding/json"
	"errors"
	"os"
	"strings"
)

// Policy is the authoritative side's hook: the far end proposes, this decides.
// A Policy sees the validated envelope and returns a Decision; Fired=false
// means the poller writes a refusal receipt and never fires.
//
// HALT — the far end's revocation (a state/halt file in the bridge, checked
// immediately before EVERY fire, poll.py-style) — is deliberately NOT
// implemented here (#1133 scope). It belongs as a Policy that denies everything
// and asks the loop to stop; wire it in with Poller.Policy when it lands.
type Policy interface {
	Decide(env *Envelope) Decision
}

// PolicyFunc adapts a function to Policy.
type PolicyFunc func(env *Envelope) Decision

// Decide implements Policy.
func (f PolicyFunc) Decide(env *Envelope) Decision { return f(env) }

// AllowAll fires everything that validated. It is the default only so a
// loopback can run with zero config; a real deployment sets a real policy.
var AllowAll Policy = PolicyFunc(func(*Envelope) Decision {
	return Decision{Fired: true, Policy: "allow-all"}
})

// FilePolicy is the reference policy: a JSON file in the bridge (default
// state/policy.json), re-read on every decision so the operator can tighten it
// without restarting the poller.
//
//	{
//	  "allow_methods":      ["GET", "POST"],                 // empty = any method
//	  "allow_url_prefixes": ["http://127.0.0.1:7070/"]      // empty = any URL
//	}
//
// A missing file means OPEN (allow); an unreadable or malformed file means
// CLOSED (deny) — a broken policy must fail safe, not open.
type FilePolicy struct {
	Path string
}

type filePolicyDoc struct {
	AllowMethods     []string `json:"allow_methods"`
	AllowURLPrefixes []string `json:"allow_url_prefixes"`
}

// Decide implements Policy.
func (p FilePolicy) Decide(env *Envelope) Decision {
	const name = "file-policy"
	b, err := os.ReadFile(p.Path)
	if errors.Is(err, os.ErrNotExist) {
		return Decision{Fired: true, Policy: name, Reason: "no policy file — open"}
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
	return Decision{Fired: true, Policy: name}
}

func containsFold(xs []string, s string) bool {
	for _, x := range xs {
		if strings.EqualFold(x, s) {
			return true
		}
	}
	return false
}
