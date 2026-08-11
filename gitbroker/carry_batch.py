#!/usr/bin/env python3
"""Carry Opus 5's first federation command batch — the first real cross-machine
round-trip. Fires each command through the WITNESS (call→direct GET witnessed,
channel→/run), resolves ${ulid.result.path} templates from prior receipts,
claims the created tab under container-claude (Opus 5's instruction, before
cmd 5), and writes results/ — which I hand back so it can tell me what it
learned that wasn't in what it sent. Command 7 is the whole experiment:
does provenance survive the machine boundary?
"""
import json, re, time, hashlib, urllib.request, os

COLLECTOR = "http://127.0.0.1:7070"
BROKER = "http://127.0.0.1:4445"
AGENT = "container-claude"
RES = os.path.expanduser("~/.8/bridge/results")
os.makedirs(RES, exist_ok=True)

# Opus 5's batch, verbatim (transcribed from its message over the held channel)
C4 = "01KZRZ0K8D292VHTM0H9S4BN5J"
BATCH = [
    {"ulid": "01KZRZ0K89M1YR3Y6880ARP3QA", "seq": 1, "atom": "call", "method": "GET", "url": COLLECTOR + "/", "why": "live self-description vs the committed one"},
    {"ulid": "01KZRZ0K8D5YWYH0MP3WRQSX4J", "seq": 2, "atom": "call", "method": "GET", "url": COLLECTOR + "/sessions", "why": "which surfaces are alive on your machine right now"},
    {"ulid": "01KZRZ0K8DQN25PSG3F9757A15", "seq": 3, "atom": "call", "method": "GET", "url": COLLECTOR + "/matrix", "why": "the UNFOUND cells — unknowable from here"},
    {"ulid": C4, "seq": 4, "atom": "channel", "session": "fox", "method": "browsingContext.create", "params": {"type": "tab"}, "why": "PURE AFFERENT — the context id exists only after the act"},
    {"ulid": "01KZRZ0K8DD1XN0FSN2VD3BNP7", "seq": 5, "atom": "channel", "session": "fox", "method": "browsingContext.navigate", "params": {"context": "${%s.result.context}" % C4, "url": COLLECTOR + "/matrix", "wait": "complete"}, "why": "the witness read through a tab the witness is watching"},
    {"ulid": "01KZRZ0K8D8ZGS694EMQA1XH6N", "seq": 6, "atom": "channel", "session": "fox", "method": "script.evaluate", "params": {"target": {"context": "${%s.result.context}" % C4}, "awaitPromise": True, "expression": "({ua:navigator.userAgent,dpr:devicePixelRatio,screen:[screen.width,screen.height],viewport:[innerWidth,innerHeight],title:document.title,url:location.href,t:performance.now()})"}, "why": "the physical shape of your machine — dpr is the one your retina clamp commit turned on"},
    {"ulid": "01KZRZ0K8DHV668J1C08STEANJ", "seq": 7, "atom": "call", "method": "GET", "url": COLLECTOR + "/manifest", "why": "THE TEST — does the manifest attribute the new tab to container-claude?"},
]

results = {}  # ulid -> parsed result body


def http(method, url, body=None):
    data = json.dumps(body).encode() if body is not None else None
    r = urllib.request.Request(url, data=data, method=method,
                               headers={"content-type": "application/json"} if data else {})
    resp = urllib.request.urlopen(r, timeout=25)
    return resp.status, dict(resp.headers), resp.read().decode()


def resolve(obj):
    """substitute ${ulid.result.path} from prior receipts."""
    if isinstance(obj, str):
        m = re.fullmatch(r"\$\{([0-9A-Z]+)\.(.+)\}", obj)
        if m:
            ul, path = m.group(1), m.group(2).split(".")
            cur = results.get(ul, {})
            for p in path:
                cur = cur.get(p, {}) if isinstance(cur, dict) else {}
            return cur
        return obj
    if isinstance(obj, dict):
        return {k: resolve(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [resolve(x) for x in obj]
    return obj


for cmd in BATCH:
    ul, seq, atom = cmd["ulid"], cmd["seq"], cmd["atom"]
    rec = {"ulid": ul, "seq": seq, "atom": atom, "why": cmd["why"], "ts": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())}
    t0 = time.time()
    try:
        if atom == "call":
            status, hdrs, body = http(cmd["method"], cmd["url"])
        else:  # channel → through the witness /run (reafference in headers)
            params = resolve(cmd.get("params", {}))
            status, hdrs, body = http("POST", COLLECTOR + "/run?session=" + cmd.get("session", "fox"),
                                      {"method": cmd["method"], "params": params})
    except Exception as e:
        rec["error"] = str(e)
        json.dump(rec, open(os.path.join(RES, ul + ".json"), "w"), indent=1)
        print("seq", seq, "ERR", e)
        continue
    rec["http"] = status
    rec["latency_ms"] = round((time.time() - t0) * 1000)
    rec["body_digest"] = hashlib.sha256(body.encode()).hexdigest()[:16]
    rec["witness_receipt"] = hdrs.get("X-8-Witness", "")
    rec["frames_seq"] = hdrs.get("X-8-Ledger", "")
    rec["result"] = body[:1400]
    try:
        results[ul] = json.loads(body)
    except Exception:
        results[ul] = {}
    json.dump(rec, open(os.path.join(RES, ul + ".json"), "w"), indent=1)
    print("seq", seq, cmd["method"], "->", status, rec["latency_ms"], "ms")

    # after the CREATE (seq 4): claim the new tab under container-claude (Opus 5's instruction, before seq 5)
    if seq == 4:
        ctx = results.get(ul, {}).get("result", {}).get("context")
        if ctx:
            http("POST", COLLECTOR + "/claim", {"ctx": ctx, "agent": AGENT})
            print("   claimed new tab", ctx[:16], "under", AGENT)

# THE TEST — command 7: does the manifest attribute the new tab to container-claude?
new_ctx = results.get(C4, {}).get("result", {}).get("context")
man = results.get("01KZRZ0K8DHV668J1C08STEANJ", {})
mine = [t for t in man.get("tabs", []) if t.get("claimed_by") == AGENT or t.get("opened_by") == AGENT]
print("\n=== THE VERDICT (Opus 5's command 7) ===")
print("new tab ctx:", (new_ctx or "?")[:20])
print("tabs attributed to container-claude:", [(t.get("ctx", "")[:14], t.get("opened_by"), t.get("claimed_by")) for t in mine][:3])
print("PROVENANCE ACROSS THE BOUNDARY:", "REAL FEDERATION — who=container-claude ✓" if mine else "anonymous — pipe works, epistemics don't (the fixable, interesting result)")
