/*
 adapters/browser — the BROWSER pack's FIREFOX capture driver (symmetric with
 encoder.html and the byod device pack). READ by 8's collector and injected into
 Firefox's chrome/parent context via Marionette (/moz/context chrome +
 /execute/async — the privileged path /procinfo uses).

 WHY HERE (not in 8): this is the capture MECHANISM. 8 only ORCHESTRATES (start/stop,
 owns the chunk sink) and CONSUMES (relays WebM to the cockpit MSE). "aperture
 controls only its OWN consumption, never the tab" — this READS the tab, never drives it.

 PROVEN FIRST (via the collector): /drawprobe -> drawSnapshot is LEAK-FREE (250 frames,
 parent -362MB); /drawprobe?p=stream -> a HiddenFrame is SAME-PROCESS (directDraw the
 parent ImageBitmap into its <canvas> + captureStream + MediaRecorder all work, NO
 JSWindowActor). So the whole pipeline is one realm:
   drawSnapshot(tab) -> HiddenFrame <canvas> -> captureStream(fps) -> MediaRecorder(vp8)
   -> ondataavailable -> POST /fxchunk -> 8 -> cockpit MSE.

 Mirror the WORKING probe exactly: use the sandbox's own gBrowser (not Services.wm,
 whose principal is stricter and denied HiddenFrame). Persist the pipeline on
 gBrowser.ownerGlobal (the chrome window) so it outlives the /execute sandbox; a
 later /execute (the stop script) reads it back. ONE recorder per stream.

 arguments: [chunkUrl, urlNeedle, fps].
*/
const cb = arguments[arguments.length - 1];
const _chunkUrl = arguments[0];
const _needle = arguments[1];
const _fps = Math.max(1, Math.min(15, arguments[2] || 5));

(async () => {
  try {
    const { HiddenFrame } = ChromeUtils.importESModule("resource://gre/modules/HiddenFrame.sys.mjs");
    const chromeWin = gBrowser.ownerDocument.defaultView; // the chrome window — persists; found the same way by the stop script

    if (chromeWin.__eightFx && chromeWin.__eightFx.stop) {
      try { chromeWin.__eightFx.stop(); } catch (e) {} // one stream at a time (or leak encoders)
    }

    const tgt = gBrowser.browsers.find(
      (b) => (b.currentURI && b.currentURI.spec || "").includes(_needle)
    );
    if (!tgt || !tgt.browsingContext || !tgt.browsingContext.currentWindowGlobal) {
      cb("ERR: no live target for needle " + _needle);
      return;
    }
    const wg = tgt.browsingContext.currentWindowGlobal;

    const hf = new HiddenFrame();
    const win = await hf.get();
    const doc = win.document;

    let bmp = await wg.drawSnapshot(null, 1, "rgb(255,255,255)");
    const canvas = doc.createElement("canvas");
    canvas.width = bmp.width;
    canvas.height = bmp.height;
    const ctx = canvas.getContext("2d", { alpha: false });
    ctx.drawImage(bmp, 0, 0);
    if (bmp.close) bmp.close();

    // a HiddenFrame is NOT composited, so a timed captureStream yields a track but
    // no frames. Pull frames MANUALLY: captureStream(0) + track.requestFrame() after
    // each draw — deterministic, works offscreen.
    const stream = canvas.captureStream(0);
    const vtrack = stream.getVideoTracks()[0];
    const mime = win.MediaRecorder.isTypeSupported("video/webm;codecs=vp8")
      ? "video/webm;codecs=vp8"
      : "video/webm";
    const rec = new win.MediaRecorder(stream, { mimeType: mime, videoBitsPerSecond: 2000000 });
    let chunks = 0, bytes = 0, errs = 0, lastErr = "";
    rec.ondataavailable = (e) => {
      if (!e.data || !e.data.size) return;
      chunks++;
      bytes += e.data.size;
      // NO keepalive: keepalive requests are capped at 64KB and a WebM cluster is
      // larger — it would silently reject. Plain POST streams the full body.
      // POST from the CHROME window (privileged, can reach localhost) — the
      // HiddenFrame's opaque about:blank principal gets a NetworkError. Same process,
      // so handing the buffer to chromeWin.fetch is free.
      e.data.arrayBuffer().then((buf) => {
        chromeWin.fetch(_chunkUrl, { method: "POST", body: buf }).catch((err) => { errs++; lastErr = "" + err; });
      });
    };
    rec.start(Math.round(1000 / _fps)); // a cluster ~each frame -> continuous stream

    let inFlight = false, alive = true;
    const id = win.setInterval(async () => {
      if (!alive || inFlight) return;
      inFlight = true;
      try {
        const b = await wg.drawSnapshot(null, 1, "rgb(255,255,255)");
        ctx.drawImage(b, 0, 0, canvas.width, canvas.height);
        if (b.close) b.close();
        try { vtrack.requestFrame(); } catch (e) {} // push one frame into the encoder
      } catch (e) {} finally { inFlight = false; }
    }, Math.round(1000 / _fps));

    chromeWin.__eightFx = {
      needle: _needle,
      stop: () => {
        alive = false;
        try { win.clearInterval(id); } catch (e) {}
        try { if (rec.state !== "inactive") rec.stop(); } catch (e) {}
        try { hf.destroy(); } catch (e) {}
        delete chromeWin.__eightFx;
      },
      stats: () => ({ needle: _needle, chunks, bytes, errs, lastErr, w: canvas.width, h: canvas.height }),
    };

    cb(JSON.stringify({ started: true, target: tgt.currentURI.spec.slice(0, 60), w: canvas.width, h: canvas.height, mime, fps: _fps }));
  } catch (e) {
    cb("ERR: " + e);
  }
})();
