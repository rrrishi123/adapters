// cmd/byod: the BYOD pack — bring a LOCAL real device up as a W3C session and
// emit a RunResult. This is the provider-specific ENDPOINT logic (adb / appium /
// iproxy) the peer placed in ADAPTERS, not pilot: pilot only DECIDES to bring a
// device up; the adapter knows HOW — the iproxy incantation, the uiautomator2 /
// XCUITest caps, the mjpeg port, the device catalog. http-mcp just carries the
// http_request to appium. 8 never does this — it only OBSERVES the session that
// lands in pilot's registry (RunResult below is the neutral contract).
//
//	byod list                  -> discover local devices (adb + go-ios), JSON
//	byod up --udid <id>        -> bring it up (appium + iproxy) -> RunResult JSON
//
// Prereqs the HOST guarantees: appium running on --hub (default :4723). For iOS,
// a valid (non-revoked) signing identity + the WDA build (Xcode once); the
// adapter then reuses it via usePrebuiltWDA and forwards the mjpeg with iproxy.
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"strings"
	"time"
)

// RunResult is the neutral contract: pilot stores it in its registry, 8 reads it
// to know a session exists, then observes it on the wire by session_id+hub.
type RunResult struct {
	SessionID string `json:"session_id"`
	Platform  string `json:"platform"` // android | ios
	Device    string `json:"device"`   // udid
	HubURL    string `json:"hub_url"`   // appium base, e.g. http://127.0.0.1:4723
	Stream    string `json:"stream,omitempty"`
	Transport string `json:"transport"` // "call" (W3C/Appium over HTTP)
}

type dev struct {
	UDID     string `json:"udid"`
	Platform string `json:"platform"`
}

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "usage: byod list | byod up --udid <id> [--hub http://127.0.0.1:4723] [--mjpeg 9100] [--team <xcodeOrgId>]")
		os.Exit(2)
	}
	switch os.Args[1] {
	case "list":
		emit(discover())
	case "up":
		fs := flag.NewFlagSet("up", flag.ExitOnError)
		udid := fs.String("udid", "", "device udid (from `byod list`)")
		hub := fs.String("hub", "http://127.0.0.1:4723", "appium base url")
		mjpeg := fs.Int("mjpeg", 0, "host mjpeg port (default 9100 android / 9101 ios)")
		team := fs.String("team", "C3CS4J5WW8", "iOS xcodeOrgId (signing team) — reused for prebuilt WDA")
		fs.Parse(os.Args[2:])
		if *udid == "" {
			fail("--udid required (see `byod list`)")
		}
		rr, err := up(*udid, *hub, *mjpeg, *team)
		if err != nil {
			fail(err.Error())
		}
		emit(rr)
	default:
		fail("unknown command " + os.Args[1])
	}
}

// discover — the device catalog: every local real device adb + go-ios can see.
func discover() []dev {
	var out []dev
	if b, err := exec.Command("adb", "devices").Output(); err == nil {
		for _, line := range strings.Split(string(b), "\n")[1:] {
			f := strings.Fields(line)
			if len(f) >= 2 && f[1] == "device" {
				out = append(out, dev{UDID: f[0], Platform: "android"})
			}
		}
	}
	if b, err := exec.Command("ios", "list").Output(); err == nil {
		var il struct {
			DeviceList []string `json:"deviceList"`
		}
		if json.Unmarshal(b, &il) == nil {
			for _, u := range il.DeviceList {
				out = append(out, dev{UDID: u, Platform: "ios"})
			}
		}
	}
	return out
}

// up — bring ONE device up as a W3C/Appium session and return the RunResult.
func up(udid, hub string, mjpeg int, team string) (*RunResult, error) {
	plat := platformOf(udid)
	if plat == "" {
		return nil, fmt.Errorf("device %s not found by adb or go-ios", udid)
	}
	if mjpeg == 0 {
		mjpeg = map[string]int{"android": 9100, "ios": 9101}[plat]
	}
	var caps string
	switch plat {
	case "android":
		caps = fmt.Sprintf(`{"capabilities":{"alwaysMatch":{"platformName":"Android","appium:automationName":"UiAutomator2","appium:udid":%q,"appium:newCommandTimeout":3600,"appium:mjpegServerPort":%d,"appium:noReset":true},"firstMatch":[{}]}}`, udid, mjpeg)
	case "ios":
		// reuse the WDA Xcode built+signed (usePrebuiltWDA) with the valid team —
		// no rebuild, no revoked-cert wall. wdaLocalPort/mjpeg are host ports.
		caps = fmt.Sprintf(`{"capabilities":{"alwaysMatch":{"platformName":"iOS","appium:automationName":"XCUITest","appium:udid":%q,"appium:newCommandTimeout":3600,"appium:mjpegServerPort":%d,"appium:wdaLocalPort":8101,"appium:xcodeOrgId":%q,"appium:xcodeSigningId":"Apple Development","appium:usePrebuiltWDA":true,"appium:useNewWDA":false,"appium:waitForQuiescence":false,"appium:autoAcceptAlerts":true,"appium:wdaLaunchTimeout":180000,"appium:noReset":true},"firstMatch":[{}]}}`, udid, mjpeg, team)
	}
	// CARRY the bring-up over the wire (plain http_request to appium — the lean wire).
	cl := &http.Client{Timeout: 240 * time.Second}
	resp, err := cl.Post(hub+"/session", "application/json", strings.NewReader(caps))
	if err != nil {
		return nil, fmt.Errorf("appium session create: %w", err)
	}
	defer resp.Body.Close()
	rb, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	var sv struct {
		Value struct {
			SessionID string `json:"sessionId"`
			Error     string `json:"error"`
			Message   string `json:"message"`
		} `json:"value"`
	}
	json.Unmarshal(rb, &sv)
	if sv.Value.SessionID == "" {
		return nil, fmt.Errorf("appium refused: %s %s", sv.Value.Error, trim(sv.Value.Message, 200))
	}
	// iOS does NOT auto-forward the mjpeg (unlike adb) — the iproxy dance is the
	// BYOD-specific bit that belongs HERE in the adapter, per the peer.
	if plat == "ios" {
		exec.Command("pkill", "-f", fmt.Sprintf("iproxy.*%d:%d", mjpeg, mjpeg)).Run()
		c := exec.Command("iproxy", "-u", udid, fmt.Sprintf("%d:%d", mjpeg, mjpeg))
		c.Stdout, c.Stderr = nil, nil
		if err := c.Start(); err != nil {
			return nil, fmt.Errorf("iproxy mjpeg forward: %w", err)
		}
	}
	return &RunResult{
		SessionID: sv.Value.SessionID, Platform: plat, Device: udid,
		HubURL: hub, Stream: fmt.Sprintf("http://127.0.0.1:%d", mjpeg), Transport: "call",
	}, nil
}

func platformOf(udid string) string {
	for _, d := range discover() {
		if d.UDID == udid {
			return d.Platform
		}
	}
	return ""
}

func emit(v any) { b, _ := json.MarshalIndent(v, "", "  "); fmt.Println(string(b)) }
func fail(m string) {
	json.NewEncoder(os.Stderr).Encode(map[string]string{"error": m})
	os.Exit(1)
}
func trim(s string, n int) string {
	if len(s) > n {
		return s[:n]
	}
	return s
}

