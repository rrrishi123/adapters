#!/usr/bin/env bash
# build.sh — canonical build for adapters (the ARMS). One module, each cmd/ main
# to its own binary in .bin/. Build with THIS, not ad-hoc `go build -o`.
set -euo pipefail
root="$(cd "$(dirname "$0")" && pwd)"
cd "$root"
mkdir -p .bin
go build -o .bin/browser  ./browser/cmd/browser    # firefox/chrome seat (BiDi/CDP)
go build -o .bin/byod     ./byod/cmd/byod
go build -o .bin/loopback ./cmd/loopback
go build -o .bin/harvest  ./lambdatest/cmd/harvest
go build -o .bin/matrix   ./lambdatest/cmd/matrix
echo "built: $root/.bin/{browser,byod,loopback,harvest,matrix}"
