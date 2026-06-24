# provider-packs

The **4th arm**. Provider-specific data + orchestration — kept *out* of the wire,
the host, and the witness so all three stay generic.

```
http-mcp        the WIRE      — two physics atoms (CALL=http_request, CHANNEL=bidi_command). Zero provider awareness.
pilot           the HOST      — local-model loop + the wire tools + shell/fs. Generic. Imports a pack to run provider flows.
8               the WITNESS   — protocol-blind cockpit over every live session. Generic. Renders provider runs, owns no provider logic.
provider-packs  the PROVIDERS — per-provider spec + runner + upload + catalog + host composers. THIS repo.
```

Why separate (end-to-end argument, narrow-waist/hourglass model, Dependency Inversion):
a function needed only by *one* provider is application logic and must live at the
endpoint, not in the universal lower layers. Baking LambdaTest into http-mcp/pilot/8
would fatten their waists and erode the moat each holds by being **general**. The
dependency flows one way: a pack depends on the wire's two physics; never the reverse.
Swap LambdaTest → BrowserStack (or a custom/BYOD backend) by swapping the pack.

## A pack's contract

Each provider directory exposes the same shape so pilot / 8 / CI consume it identically:

- **`spec.json`** — declarative map: framework → physics (call|channel), endpoint
  templates, capability mappings, artifact-fetch endpoints, app inventory.
- **`catalog.json`** — the live device/version/browser catalog (harvested).
- **a runner** (`cmd/matrix`) — takes a run request (framework, caps, app, auth
  profile) and returns a result (buildId/sessionId, status, artifact URLs). Drives
  the two physics; reads creds **below the boundary** (env / gitignored `auth/`),
  never as a CLI arg.
- **host composers** it needs (`playwright-runner.js`) — e.g. the Playwright driver
  runtime for the driver-mediated `/playwright` channel.
- **`cmd/harvest`** — refresh `catalog.json` from the provider's live catalog.

## Packs

- **`lambdatest/`** — the first pack. selenium / appium (rd+vd, app+web) / puppeteer /
  playwright (host driver) / espresso+xcui framework builds; upload (realDevice /
  virtualDevice / uploadFramework); artifact fetch (session video+logs, build artifacts).
- *next:* `browserstack/`, `sauce/`, and custom packs — `byod/` (bring-your-own real
  device over local adb/appium), own-grid, bespoke backends — each implementing the
  same contract.

## Run (lambdatest)

```
# from the repo root, creds in ./auth/<profile>.json (gitignored) or env
go run ./lambdatest/cmd/matrix --dry                 # list the curated matrix
go run ./lambdatest/cmd/matrix --duration 3m --artifacts --pw-runner lambdatest/playwright-runner.js
go run ./lambdatest/cmd/harvest                      # refresh lambdatest/catalog.json
go run ./lambdatest/cmd/matrix --upload "realDevice:/path/app.apk"
```

Auth: never read or printed. The secret stays below the boundary.
