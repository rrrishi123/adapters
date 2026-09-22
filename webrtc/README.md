# WebRTC CHANNEL adapter

`Dial(ctx, Config)` performs one adapter-owned `httpx.Do` signaling CALL and
returns a reliable, ordered DataChannel after ICE, DTLS, SCTP and DCEP complete.
`Conn` exposes `WriteText`, `ReadText`, `SetDeadline`, `Close`, and
`BidiCommand(id, method, params, onEvent)`. The latter uses the wire's
`{id,method,params}` command shape, matches the response id, and optionally
passes unmatched frames to a callback. A held connection can exchange many
commands and unsolicited events. SDP stays inside this adapter.

There is one reader per connection; command exchanges must be serialized.
Writes are safe from multiple goroutines. Messages are capped at 64 KiB;
reads skip binary messages. Deadlines bound both reads and backpressured writes.
`Dial`'s context governs negotiation; explicitly close the returned connection.

Build with `./build.sh`. A local demonstration with no external STUN:

```sh
.bin/webrtc -role answer -listen 127.0.0.1:9090 -signal-path /signal -loopback-only=true
.bin/webrtc -role dial -signaling http://127.0.0.1:9090/signal -loopback-only=true \
  -method echo -params '{"text":"hello"}'
.bin/loopback webrtc
```

These are example endpoints, not defaults. The answer command is an explicit
echo peer, supporting `echo` and a `channel.ready` event. Applications use
`NewSignaler(config, func(*Conn))` with their own channel handler; the handler
owns the channel until it returns. Closing the signaler closes all peers.

Every node chooses its signaling endpoint/address and ICE servers. CLI flags
and `WEBRTC_` environment alternatives are listed in `capabilities.json`.
Flags override the corresponding environment values. `-ice-servers` /
`WEBRTC_ICE_SERVERS` is a JSON array, for example:

```json
[{"urls":["stun:stun.example.net:3478"]},
 {"urls":["turn:turn.example.net:3478"],"username":"node","credential":"secret"}]
```

An empty list uses host candidates only; no public STUN service is assumed.
Both peers gather candidates into SDP before the single POST/answer exchange
(non-trickle ICE). `-loopback-only=true` excludes all non-loopback local and
remote candidates, disables mDNS and rejects any configured ICE servers.
HTTPS signaling is supported through Go's HTTP client; an answering handler
can be mounted behind the operator's HTTPS/authenticated server. TURN secrets
stay in node configuration, never in command frames.

The package uses [Pion WebRTC](https://github.com/pion/webrtc), pinned in the
module files. Media and SRTP are outside this DataChannel adapter's scope.
`go test ./webrtc/...` exercises actual peers, including the held channel after
signaling closes. `loopback webrtc` emits one JSON result for the witness.
