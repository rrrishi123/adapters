#!/usr/bin/env bash
# build.sh — canonical build for adapters (the ARMS). Each cmd/ main
# to its own binary in .bin/. Build with THIS, not ad-hoc `go build -o`.
# WebRTC is a separate module so only its binary depends on Pion.
set -euo pipefail
root="$(cd "$(dirname "$0")" && pwd)"
cd "$root"
mkdir -p .bin
go build -o .bin/browser  ./browser/cmd/browser    # firefox/chrome seat (BiDi/CDP)
go build -o .bin/byod     ./byod/cmd/byod
go build -o .bin/loopback ./cmd/loopback
go build -o .bin/gitbroker ./gitbroker/cmd/gitbroker   # store-and-forward relay (CALL adapter)
go build -o .bin/mqtt     ./mqtt/cmd/mqtt              # persistent broker relay (CHANNEL adapter)
(cd webrtc && go build -o "$root/.bin/webrtc" ./cmd/webrtc) # experimental transport primitive
go build -o .bin/harvest  ./lambdatest/cmd/harvest
go build -o .bin/matrix   ./lambdatest/cmd/matrix
echo "built: $root/.bin/{browser,byod,loopback,gitbroker,mqtt,harvest,matrix}"
echo "built: $root/.bin/webrtc"
