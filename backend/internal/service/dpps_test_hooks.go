package service

import (
	"net"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
)

const (
	dppsTestChatGPTBaseURLEnv       = "SUB2API_TEST_CHATGPT_BASE_URL"
	dppsTestAllowLoopbackWebhookEnv = "SUB2API_TEST_DPPS_ALLOW_LOOPBACK_WEBHOOK"
	dppsChatGPTCanonicalHost        = "chatgpt.com"
)

func dppsIsLoopbackHostname(host string) bool {
	host = strings.TrimSpace(host)
	if host == "" {
		return false
	}
	if strings.EqualFold(host, "localhost") {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

func dppsParseLoopbackTestBaseURL(raw string) *url.URL {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" || strings.Contains(trimmed, "#") || strings.Contains(trimmed, "?") {
		return nil
	}
	u, err := url.Parse(trimmed)
	if err != nil || u == nil || u.User != nil || u.Fragment != "" || u.RawQuery != "" || u.Host == "" || strings.HasSuffix(u.Host, ":") {
		return nil
	}
	if port := u.Port(); port != "" {
		p, err := strconv.Atoi(port)
		if err != nil || p < 1 || p > 65535 {
			return nil
		}
	}
	scheme := strings.ToLower(u.Scheme)
	if scheme != "http" && scheme != "https" {
		return nil
	}
	if !dppsIsLoopbackHostname(u.Hostname()) {
		return nil
	}
	u.Scheme = scheme
	return u
}

func dppsURLMatchesLoopbackOverride(reqURL, override *url.URL) bool {
	if reqURL == nil || override == nil || !dppsIsLoopbackHostname(reqURL.Hostname()) {
		return false
	}
	if !strings.EqualFold(reqURL.Scheme, override.Scheme) || !strings.EqualFold(reqURL.Host, override.Host) {
		return false
	}
	basePath := strings.TrimRight(override.Path, "/")
	if basePath != "" && reqURL.Path != basePath && !strings.HasPrefix(reqURL.Path, basePath+"/") {
		return false
	}
	return true
}

// dppsResolveChatGPTURL rewrites hardcoded ChatGPT upstream endpoints onto
// SUB2API_TEST_CHATGPT_BASE_URL when set to a strict loopback http/https base
// URL without userinfo, query, or fragment. Unset or non-loopback values are
// ignored and defaultURL is returned unchanged.
func dppsResolveChatGPTURL(defaultURL string) string {
	override := dppsParseLoopbackTestBaseURL(os.Getenv(dppsTestChatGPTBaseURLEnv))
	if override == nil {
		return defaultURL
	}
	target, err := url.Parse(strings.TrimSpace(defaultURL))
	if err != nil || target == nil {
		return defaultURL
	}
	switch strings.ToLower(target.Scheme) {
	case "ws", "wss":
		if override.Scheme == "https" {
			target.Scheme = "wss"
		} else {
			target.Scheme = "ws"
		}
	default:
		target.Scheme = override.Scheme
	}
	target.Host = override.Host
	if basePath := strings.TrimRight(override.Path, "/"); basePath != "" {
		switch {
		case target.Path == "":
			target.Path = basePath
		case strings.HasPrefix(target.Path, "/"):
			target.Path = basePath + target.Path
		default:
			target.Path = basePath + "/" + target.Path
		}
		target.RawPath = ""
	}
	return target.String()
}

// dppsApplyChatGPTHost sets req.Host to req.URL.Host when a valid loopback test
// override matches req.URL, and otherwise preserves the canonical "chatgpt.com" Host.
func dppsApplyChatGPTHost(req *http.Request) {
	if req == nil {
		return
	}
	override := dppsParseLoopbackTestBaseURL(os.Getenv(dppsTestChatGPTBaseURLEnv))
	if override != nil && dppsURLMatchesLoopbackOverride(req.URL, override) {
		req.Host = req.URL.Host
		return
	}
	req.Host = dppsChatGPTCanonicalHost
}
