package mqtt

import (
	"encoding/json"
	"errors"
	"os"
	"strings"
)

// Policy is the host's gate, both directions: the far end proposes, this
// decides whether the atom fires (inbound) and what the receipt may carry back
// toward a broker the host does not own (outbound, Decision.Egress).
type Policy interface {
	Decide(env *Envelope) Decision
}

// PolicyFunc adapts a function to Policy.
type PolicyFunc func(env *Envelope) Decision

// Decide implements Policy.
func (f PolicyFunc) Decide(env *Envelope) Decision { return f(env) }

// AllowAll fires everything that validated and lets the full (capped) receipt
// out. It is the default only so a loopback runs with zero config; a real
// deployment sets a real policy.
var AllowAll Policy = PolicyFunc(func(*Envelope) Decision {
	return Decision{Fired: true, Policy: "allow-all"}
})

// FilePolicy is the reference policy: a JSON file on the host, re-read on
// every decision so the operator can tighten it without restarting.
//
//	{
//	  "allow_atoms":           ["call", "channel"],       // empty = both
//	  "allow_agents":          ["sandbox-1"],             // empty = any agent
//	  "allow_methods":         ["GET", "POST"],           // CALL: empty = any HTTP method
//	  "allow_url_prefixes":    ["http://127.0.0.1:7070/"],// CALL: empty = any URL
//	  "allow_sessions":        ["fox"],                   // CHANNEL: empty = any seat
//	  "allow_channel_methods": ["browsingContext."],      // CHANNEL: prefix match; empty = any
//	  "egress":                {"body": true, "headers": false}  // what leaves in the receipt; absent = both
//	}
//
// A missing file means OPEN (allow, full egress); an unreadable or malformed
// file means CLOSED (deny) — a broken policy must fail safe, not open.
type FilePolicy struct {
	Path string
}

type filePolicyDoc struct {
	AllowAtoms          []string `json:"allow_atoms"`
	AllowAgents         []string `json:"allow_agents"`
	AllowMethods        []string `json:"allow_methods"`
	AllowURLPrefixes    []string `json:"allow_url_prefixes"`
	AllowSessions       []string `json:"allow_sessions"`
	AllowChannelMethods []string `json:"allow_channel_methods"`
	Egress              *Egress  `json:"egress"`
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
	deny := func(reason string) Decision { return Decision{Fired: false, Policy: name, Reason: reason} }
	if len(doc.AllowAtoms) > 0 && !containsFold(doc.AllowAtoms, env.Atom) {
		return deny("atom " + env.Atom + " not in allow_atoms")
	}
	if len(doc.AllowAgents) > 0 && !containsFold(doc.AllowAgents, env.Agent) {
		return deny("agent " + env.Agent + " not in allow_agents")
	}
	switch env.Atom {
	case AtomCall:
		if len(doc.AllowMethods) > 0 && !containsFold(doc.AllowMethods, env.Call.Method) {
			return deny("method " + env.Call.Method + " not in allow_methods")
		}
		if len(doc.AllowURLPrefixes) > 0 && !hasPrefixAny(env.Call.URL, doc.AllowURLPrefixes) {
			return deny("url not under any allow_url_prefixes")
		}
	case AtomChannel:
		if len(doc.AllowSessions) > 0 && !containsFold(doc.AllowSessions, env.Channel.Session) {
			return deny("session " + env.Channel.Session + " not in allow_sessions")
		}
		if len(doc.AllowChannelMethods) > 0 && !hasPrefixAny(env.Channel.Method, doc.AllowChannelMethods) {
			return deny("channel method " + env.Channel.Method + " not under any allow_channel_methods")
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

func hasPrefixAny(s string, prefixes []string) bool {
	for _, p := range prefixes {
		if strings.HasPrefix(s, p) {
			return true
		}
	}
	return false
}
