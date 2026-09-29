// gitbroker — the store-and-forward relay as a single portable binary.
//
//	gitbroker -bridge ~/.8/bridge -policy ~/.8/gitbroker-policy.json -slots ~/.8/gitbroker-slots.json \
//	          -collector http://127.0.0.1:7070 -actor far-end -interval 5s
//	gitbroker -bridge /tmp/b -repo git@host:org/bridge.git -branch main -policy /etc/gitbroker/policy.json -once
//
// Every value is a flag with a GITBROKER_* environment fallback; nothing points
// at a machine unless the operator says so. The bridge must exist (or -repo
// must be given to clone it).
//
// Security gate (#1145): -policy is REQUIRED and must resolve OUTSIDE the
// bridge checkout (the bridge is public; a policy inside it is a policy the
// far end can rewrite). A missing policy file is CLOSED — nothing fires.
// -slots (the auth_slot credentials) is optional but, when given, must also be
// outside the bridge. The far end's HALT is <bridge>/state/halt, checked
// before every fire.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/rrrishi123/adapters/gitbroker"
)

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func main() {
	var (
		bridge    = flag.String("bridge", envOr("GITBROKER_BRIDGE", ""), "local checkout of the bridge repo (required)")
		repo      = flag.String("repo", envOr("GITBROKER_REPO", ""), "bridge repo URL; cloned into -bridge when it has no .git")
		branch    = flag.String("branch", envOr("GITBROKER_BRANCH", ""), "bridge branch to clone/track")
		remote    = flag.String("remote", envOr("GITBROKER_REMOTE", "origin"), "git remote name")
		collector = flag.String("collector", envOr("GITBROKER_COLLECTOR", ""), "witness base URL, e.g. http://127.0.0.1:7070 (required unless -once with nothing to fire)")
		actor     = flag.String("actor", envOr("GITBROKER_ACTOR", "gitbroker"), "X-8-Actor declared on every fire")
		interval  = flag.Duration("interval", mustDuration(envOr("GITBROKER_INTERVAL", "3s")), "poll interval")
		policy    = flag.String("policy", envOr("GITBROKER_POLICY", ""), "policy JSON file (required; must be OUTSIDE -bridge; missing file = closed)")
		slots     = flag.String("slots", envOr("GITBROKER_SLOTS", ""), "auth-slot credentials file (must be OUTSIDE -bridge; never committed); empty = auth_slot envelopes are refused")
		maxBody   = flag.Int("max-body", 8192, "receipt body cap in bytes")
		once      = flag.Bool("once", false, "poll once and exit (prints the report as JSON)")
	)
	flag.Parse()
	if *bridge == "" {
		fmt.Fprintln(os.Stderr, "gitbroker: -bridge is required (or GITBROKER_BRIDGE)")
		flag.Usage()
		os.Exit(2)
	}
	if *policy == "" {
		fmt.Fprintln(os.Stderr, "gitbroker: -policy is required (or GITBROKER_POLICY) — the relay does not run open; the file must live outside -bridge")
		flag.Usage()
		os.Exit(2)
	}
	// G1: refuse a policy inside the public checkout, before touching the bridge
	fp, err := gitbroker.NewFilePolicy(*policy, *bridge)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	if _, err := os.Stat(fp.Path); err != nil {
		log.Printf("gitbroker: policy %s is not readable (%v) — the relay is CLOSED until it is", fp.Path, err)
	}
	// G2: the slots file is checked by Open the same way
	b, err := gitbroker.Open(gitbroker.Config{
		Dir: *bridge, RepoURL: *repo, Branch: *branch, Remote: *remote, Slots: *slots,
		PollInterval: *interval, Collector: *collector, Actor: *actor, MaxBody: *maxBody,
	})
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	p := &gitbroker.Poller{Bridge: b, Policy: fp, Log: log.New(os.Stderr, "gitbroker ", log.LstdFlags)}
	if err := p.Check(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	log.Printf("gitbroker bridge=%s mode=%s collector=%s actor=%s interval=%s policy=%s slots=%s halt=%s", *bridge, b.Mode(), *collector, *actor, *interval, fp.Path, orNone(b.Config().Slots), b.Config().HaltFile)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if *once {
		rep, err := p.Once(ctx)
		json.NewEncoder(os.Stdout).Encode(rep)
		if err != nil {
			log.Fatal(err)
		}
		return
	}
	if err := p.Run(ctx); err != nil && err != context.Canceled {
		log.Fatal(err)
	}
}

func orNone(s string) string {
	if s == "" {
		return "(none)"
	}
	return s
}

func mustDuration(s string) time.Duration {
	d, err := time.ParseDuration(s)
	if err != nil {
		return 3 * time.Second
	}
	return d
}
