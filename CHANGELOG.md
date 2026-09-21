# Changelog — adapters (the arms)

All four arms of the four-system (8, http-mcp, pilot, adapters) version
independently; the wire contract carries its own version (`contract.Version`
in http-mcp, mirrored by `trace.Version` here until deduplicated). Tags are
unsigned.

## v0.0.3 (unreleased, branch `release/v0.0.2`)

Release-readiness follow-ups to the v0.0.2 line:

- CI also runs `./build.sh` so the shippable `.bin/` layout is verified.
- `trace.Version` marked `TODO(contract-dedup)`: it duplicates
  http-mcp/contract's constant; one source of truth needs a module-arrow
  decision (adapters is zero-dependency).
- This changelog.

## v0.0.2 — 2026-09

- **trace**: the contract gets a version — every emitted Frame is stamped,
  `Compatible` gates replay.
- **browser pack**: uses its own Firefox profile, not an office one; gecko
  resolved from PATH only (hardcoded fallback and stale comments dropped).
- **gitbroker**: full public-git-safe command envelope; provenance survives the
  boundary.
- **hygiene**: `build.sh` (browser/byod/loopback/harvest/matrix → `.bin/`),
  the three stale in-tree binaries untracked with anchored ignores, gofmt gate,
  CI on this arm (no module cache: zero-dep module has no go.sum).
- **docs**: Reduction rules + `/sql` row synced; the count carries its own
  status.

## v0.0.1

First cut of the adapter arms: browser, byod, loopback, lambdatest harvest and
matrix, mqtt, trace.
