// Harvest the LIVE LambdaTest device + appium + browser catalog into catalog.json.
//
// This is provider-specific and lives in the pack, NOT in http-mcp (the wire is
// provider-agnostic). It hits the capabilities-generator endpoint the UI calls
// (no auth — public) and writes a normalized snapshot with no timestamp, so a
// diff means LambdaTest's catalog actually changed (new device/OS/appium/browser).
package main

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"sort"
)

func getLT(url string) []byte {
	req, _ := http.NewRequest("GET", url, nil)
	req.Header.Set("accept", "application/json")
	req.Header.Set("origin", "https://www.lambdatest.com")
	req.Header.Set("referer", "https://www.lambdatest.com/capabilities-generator/")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		panic(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		panic(fmt.Sprintf("GET %s -> %d", url, resp.StatusCode))
	}
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		panic(err)
	}
	return b
}

// ltBrands flattens {brands: {<brand>: [{name, osVersion[]}]}} -> {deviceName: [osVersion...]}.
func ltBrands(node map[string]any) map[string][]string {
	out := map[string][]string{}
	brands, _ := node["brands"].(map[string]any)
	for _, list := range brands {
		arr, _ := list.([]any)
		for _, d := range arr {
			dm, _ := d.(map[string]any)
			name, _ := dm["name"].(string)
			var vers []string
			if ov, ok := dm["osVersion"].([]any); ok {
				for _, v := range ov {
					if s, ok := v.(string); ok {
						vers = append(vers, s)
					}
				}
			}
			sort.Strings(vers)
			if name != "" {
				out[name] = vers
			}
		}
	}
	return out
}

// ltAppiumLatest flattens appiumVersions.<plat>[] -> {os_version: latest_version}.
func ltAppiumLatest(section map[string]any, plat string) map[string]string {
	out := map[string]string{}
	av, _ := section["appiumVersions"].(map[string]any)
	arr, _ := av[plat].([]any)
	for _, e := range arr {
		em, _ := e.(map[string]any)
		os, _ := em["os_version"].(string)
		latest, _ := em["latest_version"].(string)
		if os != "" {
			out[os] = latest
		}
	}
	return out
}

func main() {
	base := "https://mobile-api.lambdatest.com/mobile-automation/api/v1/capability/generator?isVirtualDevice="
	pools := map[string]any{}
	for _, p := range []struct{ key, flag string }{{"vd", "true"}, {"rd", "false"}} {
		var raw map[string]any
		if err := json.Unmarshal(getLT(base+p.flag), &raw); err != nil {
			panic(err)
		}
		entry := map[string]any{}
		if _, hasApp := raw["app"]; hasApp {
			for _, sect := range []string{"app", "web"} {
				s, _ := raw[sect].(map[string]any)
				if s == nil {
					continue
				}
				devs, _ := s["devices"].(map[string]any)
				se := map[string]any{
					"appiumLatestAndroid": ltAppiumLatest(s, "android"),
					"appiumLatestIos":     ltAppiumLatest(s, "ios"),
				}
				for plat, node := range devs {
					if nm, ok := node.(map[string]any); ok {
						se[plat] = ltBrands(nm)
					}
				}
				if vm, ok := s["vdOsBrowserMapping"]; ok {
					se["osBrowserMapping"] = vm
				}
				entry[sect] = se
			}
		} else {
			for plat, node := range raw {
				if nm, ok := node.(map[string]any); ok {
					if _, hasBrands := nm["brands"]; hasBrands {
						entry[plat] = ltBrands(nm)
					}
				}
			}
		}
		pools[p.key] = entry
	}
	out := map[string]any{
		"source":   "lambdatest capabilities-generator (live device/appium/browser catalog)",
		"endpoint": base + "{true|false}",
		"_note":    "rewritten each harvest, no timestamp inside — a diff means LambdaTest's catalog changed. pools: vd=virtual, rd=real device.",
		"pools":    pools,
	}
	b, _ := json.MarshalIndent(out, "", " ")
	// write to the pack's lambdatest/catalog.json (run from the pack root)
	path := filepath.Join("lambdatest", "catalog.json")
	if _, err := os.Stat("lambdatest"); err != nil {
		path = "catalog.json" // run from inside lambdatest/
	}
	if err := os.WriteFile(path, append(b, '\n'), 0o644); err != nil {
		panic(err)
	}
	fmt.Println("wrote", path)
}
