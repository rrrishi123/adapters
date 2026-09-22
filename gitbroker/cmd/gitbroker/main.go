// gitbroker — the store-and-forward relay as a single portable binary.
//
//	gitbroker -bridge ~/.8/bridge -collector http://127.0.0.1:7070 -actor far-end -interval 5s
//	gitbroker -bridge /tmp/b -repo git@host:org/bridge.git -branch main -once
//
// Every value is a flag with a GITBROKER_* environment fallback; nothing points
// at a machine unless the operator says so. The bridge must exist (or -repo
// must be given to clone it). Policy is state/policy.json inside the bridge
// (missing = open, malformed = closed); auth slots are state/slots.json (keep
// it git-ignored). The far end's HALT is not implemented here (#1133).
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"path/filepath"
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
		policy    = flag.String("policy", envOr("GITBROKER_POLICY", ""), "policy file; default <bridge>/state/policy.json")
		maxBody   = flag.Int("max-body", 8192, "receipt body cap in bytes")
		once      = flag.Bool("once", false, "poll once and exit (prints the report as JSON)")
	)
	flag.Parse()
	if *bridge == "" {
		fmt.Fprintln(os.Stderr, "gitbroker: -bridge is required (or GITBROKER_BRIDGE)")
		flag.Usage()
		os.Exit(2)
	}
	b, err := gitbroker.Open(gitbroker.Config{
		Dir: *bridge, RepoURL: *repo, Branch: *branch, Remote: *remote,
		PollInterval: *interval, Collector: *collector, Actor: *actor, MaxBody: *maxBody,
	})
	if err != nil {
		log.Fatal(err)
	}
	pf := *policy
	if pf == "" {
		pf = filepath.Join(*bridge, "state", "policy.json")
	}
	p := &gitbroker.Poller{Bridge: b, Policy: gitbroker.FilePolicy{Path: pf}, Log: log.New(os.Stderr, "gitbroker ", log.LstdFlags)}
	log.Printf("gitbroker bridge=%s mode=%s collector=%s actor=%s interval=%s policy=%s", *bridge, b.Mode(), *collector, *actor, *interval, pf)

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

func mustDuration(s string) time.Duration {
	d, err := time.ParseDuration(s)
	if err != nil {
		return 3 * time.Second
	}
	return d
}
