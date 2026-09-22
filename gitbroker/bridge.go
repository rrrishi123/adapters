package gitbroker

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"
)

// Config is everything the relay needs. Nothing here has a built-in value
// that points at a machine: the bridge repo, the witness and the interval
// are the operator's.
type Config struct {
	Dir     string // local checkout of the bridge repo (required)
	RepoURL string // optional: cloned into Dir when Dir has no .git
	Branch  string // optional: branch to clone/track
	Remote  string // git remote name; default "origin"

	CommandsDir string // relative to Dir; default "commands"
	ReceiptsDir string // relative to Dir; default "receipts"
	SlotsFile   string // relative to Dir; default "state/slots.json" (auth_slot → httpx.Auth, host-side only)

	PollInterval time.Duration // Poller.Run cadence; required for Run
	Collector    string        // witness base URL for CollectorFirer
	Actor        string        // X-8-Actor declared on every fire
	MaxBody      int           // receipt body cap in bytes; default 8192
	FireTimeout  time.Duration // default per-fire deadline when max_ms is 0; default 60s
}

func (c *Config) defaults() {
	if c.Remote == "" {
		c.Remote = "origin"
	}
	if c.CommandsDir == "" {
		c.CommandsDir = "commands"
	}
	if c.ReceiptsDir == "" {
		c.ReceiptsDir = "receipts"
	}
	if c.SlotsFile == "" {
		c.SlotsFile = filepath.Join("state", "slots.json")
	}
	if c.MaxBody <= 0 {
		c.MaxBody = 8192
	}
	if c.FireTimeout <= 0 {
		c.FireTimeout = 60 * time.Second
	}
}

// Bridge is the shared git repo: the store in store-and-forward. It is driven
// through the git binary (portable, zero-dep). Three modes, detected at Open:
//
//	remote  — a git repo with a remote: pull before each poll, push after receipts
//	local   — a git repo without a remote: commit receipts, no sync (loopback)
//	plain   — not a git repo at all: files only, nothing committed
type Bridge struct {
	cfg       Config
	isGit     bool
	hasRemote bool
}

// Open prepares the bridge directory, cloning RepoURL into Dir if needed.
func Open(cfg Config) (*Bridge, error) {
	cfg.defaults()
	if strings.TrimSpace(cfg.Dir) == "" {
		return nil, errors.New("gitbroker: Config.Dir is required")
	}
	if _, err := os.Stat(filepath.Join(cfg.Dir, ".git")); err != nil && cfg.RepoURL != "" {
		args := []string{"clone", "-q"}
		if cfg.Branch != "" {
			args = append(args, "-b", cfg.Branch)
		}
		args = append(args, cfg.RepoURL, cfg.Dir)
		if out, err := gitRun("", args...); err != nil {
			return nil, fmt.Errorf("gitbroker: clone: %v: %s", err, out)
		}
	}
	if err := os.MkdirAll(filepath.Join(cfg.Dir, cfg.CommandsDir), 0o755); err != nil {
		return nil, err
	}
	if err := os.MkdirAll(filepath.Join(cfg.Dir, cfg.ReceiptsDir), 0o755); err != nil {
		return nil, err
	}
	b := &Bridge{cfg: cfg}
	if _, err := gitRun(cfg.Dir, "rev-parse", "--is-inside-work-tree"); err == nil {
		b.isGit = true
		if _, err := gitRun(cfg.Dir, "remote", "get-url", cfg.Remote); err == nil {
			b.hasRemote = true
		}
	}
	return b, nil
}

// Config returns the effective (defaulted) configuration.
func (b *Bridge) Config() Config { return b.cfg }

// Mode reports remote | local | plain (see Bridge).
func (b *Bridge) Mode() string {
	switch {
	case b.isGit && b.hasRemote:
		return "remote"
	case b.isGit:
		return "local"
	default:
		return "plain"
	}
}

// Pull fast-forwards from the remote. No-op unless Mode is remote.
func (b *Bridge) Pull() error {
	if !b.hasRemote {
		return nil
	}
	if out, err := gitRun(b.cfg.Dir, "pull", "-q", "--ff-only", b.cfg.Remote); err != nil {
		return fmt.Errorf("gitbroker: pull: %v: %s", err, out)
	}
	return nil
}

// CommitReceipts stages the receipts directory and commits it. No-op unless
// the bridge is a git repo. Identity and signing are pinned per-invocation so
// the poller works on a box with no git config and no gpg agent.
func (b *Bridge) CommitReceipts(ulids []string) (string, error) {
	if !b.isGit || len(ulids) == 0 {
		return "", nil
	}
	if out, err := gitRun(b.cfg.Dir, "add", "--", b.cfg.ReceiptsDir); err != nil {
		return "", fmt.Errorf("gitbroker: add: %v: %s", err, out)
	}
	msg := "receipts: " + strings.Join(ulids, ",")
	if out, err := gitRun(b.cfg.Dir,
		"-c", "user.name=gitbroker", "-c", "user.email=gitbroker@localhost", "-c", "commit.gpgsign=false",
		"commit", "-q", "-m", msg); err != nil {
		return "", fmt.Errorf("gitbroker: commit: %v: %s", err, out)
	}
	sha, err := gitRun(b.cfg.Dir, "rev-parse", "--short", "HEAD")
	return strings.TrimSpace(sha), err
}

// Push publishes committed receipts. No-op unless Mode is remote.
func (b *Bridge) Push() error {
	if !b.hasRemote {
		return nil
	}
	if out, err := gitRun(b.cfg.Dir, "push", "-q", b.cfg.Remote); err != nil {
		return fmt.Errorf("gitbroker: push: %v: %s", err, out)
	}
	return nil
}

func (b *Bridge) commandsPath() string { return filepath.Join(b.cfg.Dir, b.cfg.CommandsDir) }
func (b *Bridge) receiptsPath() string { return filepath.Join(b.cfg.Dir, b.cfg.ReceiptsDir) }
func (b *Bridge) slotsPath() string    { return filepath.Join(b.cfg.Dir, b.cfg.SlotsFile) }

func gitRun(dir string, args ...string) (string, error) {
	cmd := exec.Command("git", args...)
	if dir != "" {
		cmd.Dir = dir
	}
	var out bytes.Buffer
	cmd.Stdout, cmd.Stderr = &out, &out
	err := cmd.Run()
	return out.String(), err
}

// InitLocal creates a fresh local-mode bridge repo at dir (git init + an empty
// first commit) — what a loopback needs to prove the commit leg without any
// remote. Fails if git is not on PATH.
func InitLocal(dir string) error {
	if _, err := exec.LookPath("git"); err != nil {
		return fmt.Errorf("gitbroker: git not on PATH: %w", err)
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	if out, err := gitRun(dir, "init", "-q"); err != nil {
		return fmt.Errorf("gitbroker: init: %v: %s", err, out)
	}
	if out, err := gitRun(dir,
		"-c", "user.name=gitbroker", "-c", "user.email=gitbroker@localhost", "-c", "commit.gpgsign=false",
		"commit", "-q", "--allow-empty", "-m", "bridge: init"); err != nil {
		return fmt.Errorf("gitbroker: first commit: %v: %s", err, out)
	}
	return nil
}
