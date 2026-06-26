package main

// App upload — the prereq-satisfaction step, in Go, below the boundary.
//
// Mirrors QA-sanctum/app_upload/app_upload.py but resolves the credential from
// the auth profile (never an argv/log): multipart POST of an apk/ipa/zip to the
// LambdaTest upload endpoint. The returned lt:// id is what the appium/framework
// combos reference. Uploaded ids are cached in .matrix/cache.json keyed by the
// app name so re-runs are idempotent (detect-then-upload, per the peer's design).

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/rrrishi123/adapters/internal/httpx"
)

// uploadEndpoint maps a kind to the LT upload URL + the framework 'type' (if any).
//
//	realDevice | virtualDevice | framework:espresso-android|xcuit-ios|flutter-android|flutter-ios
func uploadEndpoint(kind string) (url, ftype string) {
	const base = "https://manual-api.lambdatest.com/app"
	switch {
	case kind == "virtualDevice":
		return base + "/upload/virtualDevice", ""
	case strings.HasPrefix(kind, "framework"):
		if i := strings.IndexByte(kind, ':'); i >= 0 {
			ftype = kind[i+1:]
		}
		return base + "/uploadFramework", ftype
	default: // realDevice
		return base + "/upload/realDevice", ""
	}
}

// uploadApp POSTs the file as multipart (name + custom_id + appFile [+ type]) and
// returns the lt:// app id. The credential is applied below the boundary.
func uploadApp(cred httpx.Auth, kind, fpath string) (string, error) {
	url, ftype := uploadEndpoint(kind)
	f, err := os.Open(fpath)
	if err != nil {
		return "", err
	}
	defer f.Close()

	name := strings.TrimSuffix(filepath.Base(fpath), filepath.Ext(fpath))
	var buf bytes.Buffer
	w := multipart.NewWriter(&buf)
	_ = w.WriteField("name", name)
	_ = w.WriteField("custom_id", name)
	if ftype != "" {
		_ = w.WriteField("type", ftype)
	}
	fw, err := w.CreateFormFile("appFile", filepath.Base(fpath))
	if err != nil {
		return "", err
	}
	if _, err := io.Copy(fw, f); err != nil {
		return "", err
	}
	w.Close()

	req, err := http.NewRequest("POST", url, &buf)
	if err != nil {
		return "", err
	}
	req.Header.Set("Content-Type", w.FormDataContentType())
	hdr := httpx.Request{Headers: map[string]string{}}
	cred.Apply(&hdr) // Basic, below the boundary
	for k, v := range hdr.Headers {
		req.Header.Set(k, v)
	}
	resp, err := (&http.Client{Timeout: 300 * time.Second}).Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != 200 {
		return "", fmt.Errorf("HTTP %d: %s", resp.StatusCode, head(string(body), 140))
	}
	id := jsonStr(string(body), "app_id")
	if id == "" {
		return "", fmt.Errorf("no app_id in response: %s", head(string(body), 140))
	}
	return id, nil
}

// runUploads parses the --upload spec ("kind:path,kind:path,...") and uploads each,
// printing the lt:// id and caching it. Then it exits — uploads are a prereq step,
// run once; their ids go into specs/providers (the inventory the matrix reads).
func runUploads(cred httpx.Auth, spec string) {
	cache := loadCache()
	for _, pair := range strings.Split(spec, ",") {
		pair = strings.TrimSpace(pair)
		if pair == "" {
			continue
		}
		i := strings.IndexByte(pair, ':')
		if i < 0 {
			fmt.Printf("  bad --upload item %q (want kind:path)\n", pair)
			continue
		}
		kind, fpath := pair[:i], pair[i+1:]
		// framework kinds carry a sub-type after a second colon: framework:espresso-android:/path
		if strings.HasPrefix(kind, "framework") {
			if j := strings.LastIndexByte(pair, ':'); j > i {
				kind, fpath = pair[:j], pair[j+1:]
			}
		}
		base := filepath.Base(fpath)
		if id, ok := cache[base]; ok {
			fmt.Printf("  cached  %-34s %s\n", base, id)
			continue
		}
		fmt.Printf("  upload  %-34s (%s) ...\n", base, kind)
		id, err := uploadApp(cred, kind, fpath)
		if err != nil {
			fmt.Printf("    FAIL  %s\n", err)
			continue
		}
		fmt.Printf("    ok    %s\n", id)
		cache[base] = id
	}
	saveCache(cache)
}

func cachePath() string { return filepath.Join(".matrix", "cache.json") }

func loadCache() map[string]string {
	m := map[string]string{}
	if b, err := os.ReadFile(cachePath()); err == nil {
		_ = json.Unmarshal(b, &m)
	}
	return m
}

func saveCache(m map[string]string) {
	_ = os.MkdirAll(".matrix", 0o755)
	b, _ := json.MarshalIndent(m, "", " ")
	if err := os.WriteFile(cachePath(), b, 0o644); err != nil {
		fmt.Printf("  (cache write failed: %v)\n", err)
		return
	}
	fmt.Printf("  cached %d ids -> %s\n", len(m), cachePath())
}
