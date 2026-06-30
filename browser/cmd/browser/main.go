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
//	browser list                    -> CDP browsers currently reachable, JSON
//	browser up [--engine chrome] [--port 9333] [--url about:blank] [--bin <path>]
//	                                -> launch + resolve CDP ws -> RunResult JSON
//	browser down [--port 9333]      -> close that browser
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
		engine := fs.String("engine", "chrome", "chrome | chromium | edge | brave")
		port := fs.Int("port", 9333, "CDP --remote-debugging-port")
		url := fs.String("url", "about:blank", "initial url")
		bin := fs.String("bin", "", "browser binary (default: resolve from --engine)")
		broker := fs.Int("broker", 4446, "suggested channel broker port for the hint")
		fs.Parse(os.Args[2:])
		rr, err := up(*engine, *bin, *port, *url, *broker)
		if err != nil {
			fail(err.Error())
		}
		emit(rr)
	case "down":
		fs := flag.NewFlagSet("down", flag.ExitOnError)
		port := fs.Int("port", 9333, "CDP port of the browser to close")
		fs.Parse(os.Args[2:])
		emit(down(*port))
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

// discover — every CDP browser reachable on the common debug ports.
func discover() []map[string]any {
	var out []map[string]any
	for _, p := range []int{9333, 9222, 9223, 9335} {
		if v, err := httpGet(fmt.Sprintf("http://127.0.0.1:%d/json/version", p), time.Second); err == nil {
			var ver map[string]any
			if json.Unmarshal(v, &ver) == nil {
				out = append(out, map[string]any{"port": p, "engine": "chrome", "hub_url": fmt.Sprintf("http://127.0.0.1:%d", p), "version": ver["Browser"]})
			}
		}
	}
	return out
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
