# Security Policy

`adapters` are the **PROVIDERS** of the four-system: the browser pack that brings
up engine seats (Chrome/Chromium/Firefox), plus provider integrations (including
a LambdaTest catalog harvester and matrix runner).

## Supported versions

Active development is on `release/v0.0.2`; security fixes land there first.

## Trust & exposure model

- The browser pack launches a browser with a **debug port** (e.g. CDP on
  `:9333`). A CDP port is **full, unauthenticated control** of that browser —
  bind it to loopback only and never expose it.
- **Provider coupling, stated honestly:** `lambdatest/` is a concrete provider
  integration (`cmd/harvest`, `cmd/matrix`) that talks to `lambdatest.com`. It
  carries **no baked-in keys** — provider credentials are exported by the
  operator from their own provider dashboard and supplied at run time. It is one
  provider among others, wired into `build.sh`; treat its endpoints and keys as
  you would any third-party provider.

## Reporting a vulnerability

Report privately via this repository's GitHub Security Advisories
(**Security → Report a vulnerability**), or open a minimal issue describing the
*class* of problem — no live credentials or working exploit in a public issue.
