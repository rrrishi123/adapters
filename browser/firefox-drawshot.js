/*
 adapters/browser — the FIREFOX leak-free STILL primitive (the periphery sibling of
 firefox-stream.js). READ by 8's collector and run in Firefox's chrome/parent context
 via Marionette (/moz/context chrome + /execute/async), exactly like the stream driver.

 WHY IT EXISTS: the cockpit's periphery tiles used to poll /shot = BiDi
 browsingContext.captureScreenshot. BiDi holds each frame's base64 in the PARENT
 process and never releases it — so the parent climbs ~100MB/min and the watchdog
 FLOW-10-recycles Firefox every ~30min (the sawtooth). drawSnapshot is proven
 LEAK-FREE (the /drawprobe measured -362MB over 250 frames). This is the same
 primitive as the hero stream, but it returns ONE still instead of a video track —
 so the periphery gets leak-free frames at a low poll cadence with ZERO encoders.

 Mirror the stream driver: use the sandbox's own gBrowser (Services.wm's principal
 is stricter and denies HiddenFrame). A HiddenFrame content-doc canvas is proven
 UNtainted by a drawSnapshot bitmap (the stream probe did drawImage + captureStream
 on it), so toDataURL works here where a chrome XUL canvas is riskier. Persist the
 HiddenFrame+canvas on chromeWin.__eightShot so 0.5fps polling reuses one frame
 instead of allocating per shot.

 arguments: [urlNeedle, targetWidth, jpegQuality]. Returns {context,w,h,data} where
 data is a data:image/jpeg URL — the SAME shape /shot returns, so the cockpit tile
 consumes it identically.
*/
const cb = arguments[arguments.length - 1];
const _needle = arguments[0] || "";
// RENDER SCALE (0..1), not a target width. drawSnapshot allocates a compositor surface
// at THIS scale — so a small scale means a small, cheap surface. The old driver drew at
// scale 1 (full surface) then shrank the OUTPUT, which wasted the whole point: the
// parent-process surface accumulation is proportional to surface AREA. Periphery renders
// tiny (e.g. 0.18 -> ~1/30 the area) so it can stay LIVE without the memory climb; the
// hero renders larger for a crisp driving view.
const _scale = Math.max(0.05, Math.min(1, arguments[1] || 0.5));
const _q = Math.max(0.3, Math.min(0.92, arguments[2] || 0.6));

(async () => {
  try {
    const chromeWin = gBrowser.ownerDocument.defaultView; // persists across polls

    // lazily create ONE reusable HiddenFrame + canvas (content doc: proven untainted
    // by drawSnapshot, unlike a chrome XUL canvas). Reused every poll — cheap.
    let S = chromeWin.__eightShot;
    if (!S || !S.win || !S.win.document) {
      const { HiddenFrame } = ChromeUtils.importESModule("resource://gre/modules/HiddenFrame.sys.mjs");
      const hf = new HiddenFrame();
      const win = await hf.get();
      S = chromeWin.__eightShot = { hf, win, canvas: win.document.createElement("canvas") };
    }

    // match the tab by url substring (exact spec preferred, then includes)
    const bs = gBrowser.browsers.filter((b) => b.currentURI && b.currentURI.spec);
    const tgt =
      bs.find((b) => b.currentURI.spec === _needle) ||
      bs.find((b) => b.currentURI.spec.includes(_needle));
    if (!tgt || !tgt.browsingContext || !tgt.browsingContext.currentWindowGlobal) {
      cb("ERR: no live target for needle " + _needle);
      return;
    }
    const wg = tgt.browsingContext.currentWindowGlobal;

    // render AT scale — the returned bitmap (and the compositor surface behind it) is
    // already small, so nothing full-res is ever allocated.
    const bmp = await wg.drawSnapshot(null, _scale, "rgb(255,255,255)");
    const cw = Math.max(1, bmp.width);
    const ch = Math.max(1, bmp.height);
    const canvas = S.canvas;
    canvas.width = cw;
    canvas.height = ch;
    const ctx = canvas.getContext("2d", { alpha: false });
    ctx.drawImage(bmp, 0, 0);
    if (bmp.close) bmp.close(); // release the bitmap — the whole point

    const data = canvas.toDataURL("image/jpeg", _q);
    cb(JSON.stringify({ context: _needle, w: cw, h: ch, data }));
  } catch (e) {
    cb("ERR: " + e);
  }
})();
