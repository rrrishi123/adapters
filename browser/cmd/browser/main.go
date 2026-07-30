// cmd/browser: the BROWSER pack — bring a LOCAL browser up as a CDP-driveable
// session and emit a RunResult, symmetric with cmd/byod (the device pack). pilot
// only DECIDES to bring a browser up; THIS adapter knows HOW — the chrome launch
// flags, --remote-debugging-port, resolving the page's CDP websocket. http-mcp
// then HOLDS that socket as a channel broker; 8 OBSERVES the session that lands in
// pilot's registry. 8 never launches browsers — it only watches. This is the
// second browser engine beside the channel Firefox: per-ENGINE capture, exactly
// as byod is per-DEVICE (android mjpeg vs ios WDA). Firefox streams via its BiDi
// captureScreenshot loop; Chrome via CDP (Page.captureScreenshot now, the
// efficient Page.startScreencast push later — the Chrome twin of drawSnapshot).
//
//	browser list                    -> CDP/WebDriver browsers currently reachable, JSON
//	browser up [--engine chrome] [--port 9333] [--url about:blank] [--bin <path>]
//	                                -> launch + resolve CDP ws -> RunResult JSON
//	browser up --engine firefox [--port 4444] [--profile <dir>]
//	                                -> geckodriver + BiDi session -> RunResult JSON
//	browser down [--port 9333]      -> close that browser
//
// Firefox is engine #1, not a special case: it launches DETACHED (its own process
// session), so no shell — and no repo-8 script — is load-bearing for its lifetime.
// The seat survives the terminal that created it and is re-discoverable via `list`.
//
// RunResult.Stream is the page CDP websocket; pilot hands it to a channel broker
// (`channel -ws <stream> -listen :4446`) and adds chrome=<broker> to 8's
// collector -brokers. Then 8's rail shows a chrome seat beside the fox seats.
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"runtime"
	"strings"
	"syscall"
	"time"
)

// RunResult is the neutral contract (mirrors byod's): pilot stores it, 8 reads it
// to know a session exists, then observes it on the wire by the CDP endpoint.
type RunResult struct {
	SessionID  string `json:"session_id"` // CDP page/target id
	Engine     string `json:"engine"`     // chrome | chromium | edge | brave
	HubURL     string `json:"hub_url"`    // CDP base, http://127.0.0.1:<port>
	Stream     string `json:"stream"`     // the page CDP websocket — broker + screencast this
	Transport  string `json:"transport"`  // "channel" (CDP over ws)
	PID        int    `json:"pid,omitempty"`
	BrokerHint string `json:"broker_hint,omitempty"` // the channel invocation pilot should run
}

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "usage: browser list | browser up [--engine chrome] [--port 9333] [--url about:blank] [--bin <path>] | browser down [--port 9333]")
		os.Exit(2)
	}
	switch os.Args[1] {
	case "list":
		emit(discover())
	case "up":
		fs := flag.NewFlagSet("up", flag.ExitOnError)
		engine := fs.String("engine", "chrome", "chrome | chromium | edge | brave | firefox")
		port := fs.Int("port", 0, "CDP --remote-debugging-port (chrome family, default 9333) or geckodriver port (firefox, default 4444)")
		url := fs.String("url", "about:blank", "initial url")
		bin := fs.String("bin", "", "browser/driver binary (default: resolve from --engine)")
		broker := fs.Int("broker", 0, "suggested channel broker port for the hint (default 4446 chrome, 4445 firefox)")
		profile := fs.String("profile", "", "firefox: profile dir (default ~/.ltqa-firefox-deepseek — the persistent logged-in seat)")
		fs.Parse(os.Args[2:])
		var rr *RunResult
		var err error
		if *engine == "firefox" {
			rr, err = upFirefox(*bin, orDefault(*port, 4444), *profile, orDefault(*broker, 4445))
		} else {
			rr, err = up(*engine, *bin, orDefault(*port, 9333), *url, orDefault(*broker, 4446))
		}
		if err != nil {
			fail(err.Error())
		}
		emit(rr)
	case "down":
		fs := flag.NewFlagSet("down", flag.ExitOnError)
		engine := fs.String("engine", "chrome", "chrome family | firefox")
		port := fs.Int("port", 0, "CDP port (chrome, default 9333) or geckodriver port (firefox, default 4444)")
		fs.Parse(os.Args[2:])
		if *engine == "firefox" {
			emit(downFirefox(orDefault(*port, 4444)))
		} else {
			emit(down(orDefault(*port, 9333)))
		}
	default:
		fail("unknown command " + os.Args[1])
	}
}

type cdpTarget struct {
	ID                   string `json:"id"`
	Type                 string `json:"type"`
	URL                  string `json:"url"`
	Title                string `json:"title"`
	WebSocketDebuggerURL string `json:"webSocketDebuggerUrl"`
}

// discover — every CDP browser reachable on the common debug ports, plus the
// firefox seat (geckodriver /status + the published session in ~/.8/gecko.json).
func discover() []map[string]any {
	var out []map[string]any
	for _, p := range []int{9333, 9223, 9335} {
		if v, err := httpGet(fmt.Sprintf("http://127.0.0.1:%d/json/version", p), time.Second); err == nil {
			var ver map[string]any
			if json.Unmarshal(v, &ver) == nil {
				out = append(out, map[string]any{"port": p, "engine": "chrome", "hub_url": fmt.Sprintf("http://127.0.0.1:%d", p), "version": ver["Browser"]})
			}
		}
	}
	for _, p := range []int{4444} {
		if v, err := httpGet(fmt.Sprintf("http://127.0.0.1:%d/status", p), time.Second); err == nil {
			var st struct {
				Value struct {
					Ready   bool   `json:"ready"`
					Message string `json:"message"`
				} `json:"value"`
			}
			if json.Unmarshal(v, &st) == nil {
				e := map[string]any{"port": p, "engine": "firefox", "hub_url": fmt.Sprintf("http://127.0.0.1:%d", p), "driver_ready": st.Value.Ready}
				if seat, err := os.ReadFile(seatFile()); err == nil {
					var s map[string]any
					if json.Unmarshal(seat, &s) == nil {
						e["session_id"], e["stream"] = s["session_id"], s["ws"]
					}
				}
				out = append(out, e)
			}
		}
	}
	return out
}

func orDefault(v, d int) int {
	if v == 0 {
		return d
	}
	return v
}

// seatFile — the published firefox seat. The collector already auto-recovers from
// this file after a recycle; pilot will eventually own this registry.
func seatFile() string { return os.Getenv("HOME") + "/.8/gecko.json" }

// upFirefox — the firefox seat: geckodriver + a BiDi WebDriver session on the
// persistent profile. Fully detached (Setsid): the seat's lifetime is its own,
// not the launching shell's. Symmetric with chrome's up(), different transport
// (BiDi ws vs CDP ws) — the channel broker holds either.
func upFirefox(bin string, port int, profile string, brokerPort int) (*RunResult, error) {
	if bin == "" {
		var err error
		if bin, err = resolveGecko(); err != nil {
			return nil, err
		}
	}
	if profile == "" {
		profile = os.Getenv("HOME") + "/.ltqa-firefox-deepseek"
	}

	// replace any stale seat on this port — same restart-fresh semantics as up.sh had.
	_ = exec.Command("pkill", "-f", fmt.Sprintf("geckodriver --port %d", port)).Run()
	_ = exec.Command("pkill", "-f", "firefox.*"+profile).Run()
	time.Sleep(500 * time.Millisecond)
	_ = os.Remove(profile + "/.parentlock")

	logf, _ := os.Create("/tmp/geckodriver.log")
	cmd := exec.Command(bin, "--port", fmt.Sprint(port), "--host", "127.0.0.1",
		"--allow-hosts", "localhost", "127.0.0.1", "--log", "info")
	cmd.Stdout, cmd.Stderr = logf, logf
	// MOZ_HEADLESS set to ANY non-empty value — even "0" — makes Firefox headless
	// (it never parses the value). Scrub it so the seat always gets a real window.
	env := os.Environ()[:0:0]
	for _, kv := range os.Environ() {
		if !strings.HasPrefix(kv, "MOZ_HEADLESS=") {
			env = append(env, kv)
		}
	}
	cmd.Env = env
	cmd.SysProcAttr = &syscall.SysProcAttr{Setsid: true} // detach: survives the shell
	if err := cmd.Start(); err != nil {
		return nil, fmt.Errorf("launch geckodriver: %w", err)
	}
	pid := cmd.Process.Pid
	go cmd.Wait() // reap if it exits while we're alive; Setsid orphans it cleanly after

	base := fmt.Sprintf("http://127.0.0.1:%d", port)
	ready := false
	for i := 0; i < 50; i++ { // ~5s
		if _, err := httpGet(base+"/status", 500*time.Millisecond); err == nil {
			ready = true
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if !ready {
		return nil, fmt.Errorf("geckodriver not ready on %s after 5s (log: /tmp/geckodriver.log)", base)
	}

	caps := fmt.Sprintf(`{"capabilities":{"alwaysMatch":{"browserName":"firefox","webSocketUrl":true,
	  "moz:firefoxOptions":{"args":["-profile","%s","-remote-allow-system-access"]}}}}`, profile)
	body, err := httpPost(base+"/session", caps, 90*time.Second)
	if err != nil {
		return nil, fmt.Errorf("firefox session: %w", err)
	}
	var resp struct {
		Value struct {
			SessionID    string `json:"sessionId"`
			Capabilities struct {
				WebSocketURL string `json:"webSocketUrl"`
			} `json:"capabilities"`
		} `json:"value"`
	}
	if err := json.Unmarshal(body, &resp); err != nil || resp.Value.Capabilities.WebSocketURL == "" {
		return nil, fmt.Errorf("firefox session gave no BiDi websocket: %s", string(body[:min(len(body), 400)]))
	}
	sid, ws := resp.Value.SessionID, resp.Value.Capabilities.WebSocketURL

	// publish the seat atomically so the collector (and anyone else) can recover it.
	_ = os.MkdirAll(os.Getenv("HOME")+"/.8", 0o755)
	seat := fmt.Sprintf(`{"session_id":"%s","ws":"%s","ts":"%s"}`+"\n", sid, ws, time.Now().UTC().Format(time.RFC3339))
	tmp := seatFile() + ".tmp"
	if os.WriteFile(tmp, []byte(seat), 0o644) == nil {
		_ = os.Rename(tmp, seatFile())
	}

	return &RunResult{
		SessionID: sid, Engine: "firefox", HubURL: base,
		Stream: ws, Transport: "channel", PID: pid,
		BrokerHint: fmt.Sprintf("channel -ws %s -listen :%d   # then add fox=http://127.0.0.1:%d to 8's collector -brokers", ws, brokerPort, brokerPort),
	}, nil
}

// downFirefox — reclaim the firefox seat: driver, browser, and the published record.
func downFirefox(port int) map[string]any {
	res := map[string]any{"port": port, "engine": "firefox"}
	res["was_reachable"] = func() bool {
		_, e := httpGet(fmt.Sprintf("http://127.0.0.1:%d/status", port), time.Second)
		return e == nil
	}()
	err := exec.Command("pkill", "-f", fmt.Sprintf("geckodriver --port %d", port)).Run()
	_ = exec.Command("pkill", "-f", "firefox.*ltqa-firefox").Run()
	_ = os.Remove(seatFile())
	res["reclaimed"] = err == nil
	return res
}

// resolveGecko — PATH first, then the driver lent from ltqa-platform (all local
// browser drivers are borrowed from there today; http-mcp needs them all).
func resolveGecko() (string, error) {
	if p, err := exec.LookPath("geckodriver"); err == nil {
		return p, nil
	}
	lent := os.Getenv("HOME") + "/Desktop/repos/ltqa-platform/.bin/drivers/firefox/0.37.0/geckodriver"
	if _, err := os.Stat(lent); err == nil {
		return lent, nil
	}
	return "", fmt.Errorf("no geckodriver on PATH and none lent at %s — pass --bin", lent)
}

// up — launch the browser with CDP and resolve a page websocket to broker.
func up(engine, bin string, port int, url string, brokerPort int) (*RunResult, error) {
	if bin == "" {
		var err error
		if bin, err = resolveBin(engine); err != nil {
			return nil, err
		}
	}
	// a dedicated profile so this SUBJECT browser is isolated from any cockpit
	// browser (and from the user's daily Chrome) — same hygiene as byod's per-run.
	profile := fmt.Sprintf("%s/.8-browser-%s-%d", os.Getenv("HOME"), engine, port)
	args := []string{
		fmt.Sprintf("--remote-debugging-port=%d", port),
		"--user-data-dir=" + profile,
		"--no-first-run", "--no-default-browser-check", "--disable-features=Translate",
		"--remote-allow-origins=*", // CDP refuses ws upgrades from unlisted origins since Chrome 111
		url,
	}
	cmd := exec.Command(bin, args...)
	cmd.Stdout, cmd.Stderr = nil, nil
	if err := cmd.Start(); err != nil {
		return nil, fmt.Errorf("launch %s: %w", engine, err)
	}
	pid := cmd.Process.Pid

	// poll CDP until the page target is up and exposes its websocket.
	base := fmt.Sprintf("http://127.0.0.1:%d", port)
	deadline := time.Now()
	var page *cdpTarget
	for i := 0; i < 100; i++ { // ~10s
		if b, err := httpGet(base+"/json", 800*time.Millisecond); err == nil {
			var ts []cdpTarget
			if json.Unmarshal(b, &ts) == nil {
				for i := range ts {
					if ts[i].Type == "page" && ts[i].WebSocketDebuggerURL != "" {
						page = &ts[i]
						break
					}
				}
			}
		}
		if page != nil {
			break
		}
		time.Sleep(100 * time.Millisecond)
		deadline = time.Now()
	}
	_ = deadline
	if page == nil {
		return nil, fmt.Errorf("%s came up but no CDP page target on %s after 10s", engine, base)
	}
	return &RunResult{
		SessionID: page.ID, Engine: engine, HubURL: base,
		Stream: page.WebSocketDebuggerURL, Transport: "channel", PID: pid,
		BrokerHint: fmt.Sprintf("channel -ws %s -listen :%d   # then add chrome=http://127.0.0.1:%d to 8's collector -brokers", page.WebSocketDebuggerURL, brokerPort, brokerPort),
	}, nil
}

// down — close the browser holding this CDP port (Browser.close, then the proc).
func down(port int) map[string]any {
	base := fmt.Sprintf("http://127.0.0.1:%d", port)
	res := map[string]any{"port": port}
	// CDP's HTTP endpoint can close it gracefully via the browser ws; simplest
	// portable reclaim is to match the launch flag, like byod's port-pattern kill.
	err := exec.Command("pkill", "-f", fmt.Sprintf("remote-debugging-port=%d", port)).Run()
	res["reclaimed"] = err == nil
	res["was_reachable"] = func() bool { _, e := httpGet(base+"/json/version", time.Second); return e == nil }()
	return res
}

func resolveBin(engine string) (string, error) {
	mac := map[string][]string{
		"chrome":   {"/Applications/Google Chrome.app/Contents/MacOS/Google Chrome"},
		"chromium": {"/Applications/Chromium.app/Contents/MacOS/Chromium"},
		"edge":     {"/Applications/Microsoft Edge.app/Contents/MacOS/Microsoft Edge"},
		"brave":    {"/Applications/Brave Browser.app/Contents/MacOS/Brave Browser"},
	}
	lin := map[string][]string{
		"chrome":   {"google-chrome", "google-chrome-stable"},
		"chromium": {"chromium", "chromium-browser"},
		"edge":     {"microsoft-edge"},
		"brave":    {"brave-browser"},
	}
	cands := mac[engine]
	if runtime.GOOS != "darwin" {
		cands = lin[engine]
	}
	for _, c := range cands {
		if strings.HasPrefix(c, "/") {
			if _, err := os.Stat(c); err == nil {
				return c, nil
			}
		} else if p, err := exec.LookPath(c); err == nil {
			return p, nil
		}
	}
	return "", fmt.Errorf("no binary for engine %q (looked for %v) — pass --bin", engine, cands)
}

func httpPost(url, body string, to time.Duration) ([]byte, error) {
	cl := &http.Client{Timeout: to}
	resp, err := cl.Post(url, "application/json", strings.NewReader(body))
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	return io.ReadAll(io.LimitReader(resp.Body, 1<<20))
}

func httpGet(url string, to time.Duration) ([]byte, error) {
	cl := &http.Client{Timeout: to}
	resp, err := cl.Get(url)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	return io.ReadAll(io.LimitReader(resp.Body, 1<<20))
}

func emit(v any) { b, _ := json.MarshalIndent(v, "", "  "); fmt.Println(string(b)) }
func fail(m string) {
	json.NewEncoder(os.Stderr).Encode(map[string]string{"error": m})
	os.Exit(1)
}
