# Changelog — adapters (the arms)

All four arms of the four-system (8, http-mcp, pilot, adapters) version
independently; the wire contract carries its own version (`contract.Version`
in http-mcp, mirrored by `trace.Version` here until deduplicated). Tags are
unsigned.

## v0.0.3 (unreleased, branch `release/v0.0.2`)

Release-readiness follow-ups to the v0.0.2 line:

- **module hygiene** (#1146): WebRTC is an `experimental-primitive` echo peer,
  not a relay adapter: no witness, policy or application authentication.
  Its nested `webrtc/go.mod` and `webrtc/go.sum` isolate Pion; the parent
  adapters module (including gitbroker and mqtt) is stdlib-only.
  `build.sh` builds both modules; `loopback webrtc` invokes the separate
  WebRTC binary. CI builds, vets and tests both modules. gitbroker and mqtt
  capability descriptors now report `implemented` and `stdlib_only: true`.
- **relay security gate** (#1145, gitbroker + mqtt, new `internal/guard`):
  a cowork review proved five exploits against the relays; all closed before
  any relay ships. G1 — `-policy` is REQUIRED, must resolve OUTSIDE the
  bridge checkout (symlinks followed; gitbroker refuses to start otherwise),
  and a MISSING policy file is CLOSED; a `Poller`/`Host` with no Policy is
  `Closed`, not `AllowAll` (loopbacks set `AllowAll` explicitly). G2 —
  gitbroker gains `-slots` / `Config.Slots`; the credentials file must also
  be outside the bridge (`Open` refuses), mqtt refuses either file inside the
  enclosing git tree. G3 — receipts echo only ALLOWLISTED response headers
  (Content-Type, Content-Length, X-8-*; a Set-Cookie never leaves the host),
  and a slot-authenticated call publishes only the body's sha256 unless the
  policy opts in (`egress.authenticated_body`); receipts carry
  `authenticated`. G4 — the three-name credential denylist is replaced by a
  request-header ALLOWLIST, and URLs with userinfo (`user:key@host`) or a
  credential-named query parameter (`?access_key=`, `token`, `sig`, ...) or
  a non-http(s) scheme are rejected structurally. G5 — HALT (`<bridge>/
  state/halt`, the far end's revocation) is ported into the Go poller and
  checked immediately before EVERY fire; the legacy `gitbroker/poll.py` is
  DELETED so two pollers with different safety cannot coexist. mqtt's
  descriptor no longer overclaims: the broker session is CHANNEL-shaped, but
  every atom the adapter puts on the wire is an `http_request` to the
  witness (`wire_atoms: ["http_request"]`). Tests prove each gate.
- **webrtc** (#1135): replace the signaling-only harness with a real Pion
  DataChannel CHANNEL transport primitive (`webrtc/`, `.bin/webrtc`, `loopback webrtc`).
  SDP + gathered ICE candidates exchange in one adapter-owned `httpx` CALL;
  ICE/DTLS/SCTP opens a reliable, ordered wsx-style connection, reduced to
  `bidi_command` with command-id matching and peer events. Signaling endpoints
  and ICE/STUN/TURN servers are per-node flags with `WEBRTC_*` fallbacks.
  The localhost loopback closes signaling before three command round-trips
  and a peer event; no external STUN. Pion dependencies are isolated in
  `webrtc/go.mod`/`webrtc/go.sum`; SDP never enters the wire API.

- CI also runs `./build.sh` so the shippable `.bin/` layout is verified.
- `trace.Version` marked `TODO(contract-dedup)`: it duplicates
  http-mcp/contract's constant; one source of truth needs a module-arrow
  decision (the parent adapters module is stdlib-only; the nested WebRTC
  module has external dependencies).
- This changelog.
- **gitbroker** (#1133): the store-and-forward relay is now a Go CALL adapter
  (`gitbroker/`, binary `.bin/gitbroker`, `loopback gitbroker`): envelope +
  receipt JSON schemas, thick receipts carrying the X-8-Witness line, a
  `Policy` hook (far end proposes, host decides; `state/policy.json` reference
  policy; HALT deliberately not yet), host-side `auth_slot` resolution,
  `capabilities.json`. `poll.py` remained as the August prototype (deleted
  in #1145 once HALT was ported).
- **mqtt** (#1134): the persistent broker relay is now a Go CHANNEL adapter
  (`mqtt/`, binary `.bin/mqtt`, `loopback mqtt`): both nodes dial OUT to a
  broker (sandbox-safe), envelopes on `<prefix>/commands/<ulid>` carry a CALL
  (`http_request`) or a seat-addressed CHANNEL command (`bidi_command`), the
  host fires through the witness and publishes a thick RETAINED receipt on
  `<prefix>/receipts/<ulid>`; QoS 1 + persistent session = store-and-forward
  while the host is down; idempotent by ULID; `Policy` gates both directions
  (fire + egress); auth slots host-side; retained presence + last-will. The
  stdlib client/broker grew QoS 1, retain, wildcards, keep-alive, will,
  auth, TLS and persistent sessions (QoS 2 deliberately not offered). Broker
  URL / prefix / node are flags with `MQTT_*` fallbacks; nothing hardcoded.

## v0.0.2 — 2026-09

- **trace**: the contract gets a version — every emitted Frame is stamped,
  `Compatible` gates replay.
- **browser pack**: uses its own Firefox profile, not an office one; gecko
  resolved from PATH only (hardcoded fallback and stale comments dropped).
- **gitbroker**: full public-git-safe command envelope; provenance survives the
  boundary.
- **hygiene**: `build.sh` (browser/byod/loopback/harvest/matrix → `.bin/`),
  the three stale in-tree binaries untracked with anchored ignores, gofmt gate,
  CI on this arm (no module cache: the stdlib-only parent module has no go.sum).
- **docs**: Reduction rules + `/sql` row synced; the count carries its own
  status.

## v0.0.1

First cut of the adapter arms: browser, byod, loopback, lambdatest harvest and
matrix, mqtt, trace.
