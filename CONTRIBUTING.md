# Contributing

## Build

`./build.sh` produces SHA-stamped binaries into `.bin/` — including the browser
pack (`.bin/browser`) and the provider tools (`.bin/harvest`, `.bin/matrix`). The
boot path builds `.bin/browser` on demand; never commit built binaries, and keep
Linux (not just macOS) working since seats run cross-platform.

## Branch model

Work targets `release/v0.0.2`. Do not push to `main` or mint tags without an
explicit release decision.

## Conventions

- `adapters` are PROVIDERS, below the wire: they bring up engine seats and talk
  to external providers. A new engine or provider is additive — keep the browser
  pack's `--engine`/`--bin`/`--port` contract stable.
- Provider integrations supply their own credentials at run time; never bake keys
  into the repo.

## Pull requests

Small, reviewable commits. `gofmt` clean, `go vet` clean, `go test ./...` green.
