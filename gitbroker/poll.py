#!/usr/bin/env python3
"""git-broker poller (#17) — git as an async CHANNEL.

The far end (a container with no inbound + github-only egress, e.g. claude-web)
commits a COMMAND as a file; this host-side poller reads it, fires it on the
REAL WITNESSED Firefox, and commits back a RECEIPT. That makes the container a
NODE running the four-body system, reaching this Firefox over the one wire its
walls allow — git.

Co-designed with the far-end Claude (Opus 5). Its spec, enforced here:
  - HALT is the far end's revocation — checked immediately before EVERY fire.
  - POLICY (state/policy.json) is a host-side verb allowlist + per-window budget;
    enforcement lives host-side because that is where the trust is.
  - the tab is LEASED: after a create we POST /claim {ctx, agent} so the far end's
    tabs are held under the same mechanism as any local agent.
  - the RECEIPT carries the X-8-Witness reafference — "the afferent leg is the
    whole point; if the receipt is thin, the channel is theater with extra steps."
"""
import json, os, sys, time, hashlib, subprocess, urllib.request

BRIDGE = os.environ.get("BRIDGE", os.path.expanduser("~/.8/bridge"))
COLLECTOR = os.environ.get("COLLECTOR", "http://127.0.0.1:7070")
AGENT = os.environ.get("GB_AGENT", "container-claude")


def post(url, body):
    r = urllib.request.Request(url, data=json.dumps(body).encode(),
                               headers={"content-type": "application/json"})
    resp = urllib.request.urlopen(r, timeout=25)
    return resp.status, dict(resp.headers), resp.read().decode()


def git(*args):
    try:
        subprocess.run(["git", "-C", BRIDGE, *args], capture_output=True, timeout=30)
    except Exception:
        pass


def halted():
    return os.path.exists(os.path.join(BRIDGE, "state", "halt"))


def process_once():
    git("pull", "--quiet")
    cdir = os.path.join(BRIDGE, "commands")
    rdir = os.path.join(BRIDGE, "results")
    os.makedirs(rdir, exist_ok=True)
    policy = {}
    pf = os.path.join(BRIDGE, "state", "policy.json")
    if os.path.exists(pf):
        try:
            policy = json.load(open(pf))
        except Exception:
            policy = {}
    allow = set(policy.get("allow", []))
    done = []
    for fn in sorted(os.listdir(cdir)) if os.path.isdir(cdir) else []:
        if not fn.endswith(".json"):
            continue
        ulid = fn[:-5]
        rpath = os.path.join(rdir, ulid + ".json")
        if os.path.exists(rpath):
            continue  # already processed — idempotent
        # HALT — the far end's revocation, checked before EVERY fire
        if halted():
            print("HALTED — refusing to fire (state/halt present)")
            break
        try:
            cmd = json.load(open(os.path.join(cdir, fn)))
        except Exception as e:
            json.dump({"ulid": ulid, "error": "unparseable command: " + str(e)}, open(rpath, "w"), indent=1)
            done.append(ulid)
            continue
        method = cmd.get("method", "")
        rec = {"ulid": ulid, "method": method,
               "ts": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())}
        # POLICY allowlist (host-side trust)
        if allow and method not in allow:
            rec["error"] = "method '%s' not in policy allowlist" % method
            json.dump(rec, open(rpath, "w"), indent=1)
            done.append(ulid)
            print("REFUSED (policy):", ulid, method)
            continue
        # FIRE through the WITNESS so the receipt carries the X-8-Witness reafference
        t0 = time.time()
        try:
            status, hdrs, body = post(COLLECTOR + "/run?session=fox", cmd)
        except Exception as e:
            rec["error"] = "fire failed: " + str(e)
            json.dump(rec, open(rpath, "w"), indent=1)
            done.append(ulid)
            continue
        rec["http"] = status
        rec["latency_ms"] = round((time.time() - t0) * 1000)
        rec["body_digest"] = hashlib.sha256(body.encode()).hexdigest()[:16]
        rec["witness_receipt"] = hdrs.get("X-8-Witness", "")   # the afferent leg
        rec["frames_seq"] = hdrs.get("X-8-Ledger", "")         # seq gap → far end can refuse
        rec["result"] = body[:800]
        # a created tab is LEASED to the far end (Opus 5's requirement)
        try:
            ctx = json.loads(body).get("result", {}).get("context")
            if method == "browsingContext.create" and ctx:
                post(COLLECTOR + "/claim", {"ctx": ctx, "agent": AGENT})
                rec["claimed_ctx"] = ctx
        except Exception:
            pass
        json.dump(rec, open(rpath, "w"), indent=1)
        done.append(ulid)
        print("processed", ulid, method, "->", rec.get("claimed_ctx") or ("http " + str(rec["http"])))
    if done:
        git("add", "results")
        git("commit", "-q", "-m", "receipts: " + ",".join(done))
        git("push", "--quiet")
    return done


if __name__ == "__main__":
    once = "--once" in sys.argv
    while True:
        process_once()
        if once:
            break
        time.sleep(3)
