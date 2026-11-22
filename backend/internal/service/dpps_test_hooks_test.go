package service

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/imroc/req/v3"
	"github.com/stretchr/testify/require"
)

type dppsTestSettingRepo struct {
	SettingRepository
	mu   sync.Mutex
	data map[string]string
}

func (r *dppsTestSettingRepo) GetValue(_ context.Context, key string) (string, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	v, ok := r.data[key]
	if !ok {
		return "", ErrSettingNotFound
	}
	return v, nil
}

func (r *dppsTestSettingRepo) Set(_ context.Context, key, value string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.data == nil {
		r.data = map[string]string{}
	}
	r.data[key] = value
	return nil
}

type dppsTestOutboxRepo struct {
	mu   sync.Mutex
	recs map[string]*DppsEventOutboxRecord
	ord  []string
	sent map[string]bool
}

func newDppsTestOutboxRepo() *dppsTestOutboxRepo {
	return &dppsTestOutboxRepo{
		recs: map[string]*DppsEventOutboxRecord{},
		sent: map[string]bool{},
	}
}

func (o *dppsTestOutboxRepo) Insert(_ context.Context, rec DppsEventOutboxRecord) error {
	o.mu.Lock()
	defer o.mu.Unlock()
	if _, exists := o.recs[rec.ID]; exists {
		return nil
	}
	copyRec := rec
	o.recs[rec.ID] = &copyRec
	o.ord = append(o.ord, rec.ID)
	return nil
}

func (o *dppsTestOutboxRepo) ListDue(_ context.Context, now time.Time, limit int) ([]DppsEventOutboxRecord, error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	var out []DppsEventOutboxRecord
	for _, id := range o.ord {
		if o.sent[id] {
			continue
		}
		r := o.recs[id]
		if !r.NextAttemptAt.After(now) && len(out) < limit {
			out = append(out, *r)
		}
	}
	return out, nil
}

func (o *dppsTestOutboxRepo) MarkSent(_ context.Context, id string, _ time.Time) error {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.sent[id] = true
	return nil
}

func (o *dppsTestOutboxRepo) Reschedule(_ context.Context, id string, attempts int, next time.Time, _ string) error {
	o.mu.Lock()
	defer o.mu.Unlock()
	if r, ok := o.recs[id]; ok {
		r.Attempts = attempts
		r.NextAttemptAt = next
	}
	return nil
}

func (o *dppsTestOutboxRepo) MarkFailed(_ context.Context, id string, attempts int, _ string, _ time.Time) error {
	o.mu.Lock()
	defer o.mu.Unlock()
	if r, ok := o.recs[id]; ok {
		r.Attempts = attempts
	}
	o.sent[id] = true
	return nil
}

func (o *dppsTestOutboxRepo) Prune(context.Context, time.Time) (int64, error) { return 0, nil }

type dppsTestEncryptor struct{}

func (dppsTestEncryptor) Encrypt(plain string) (string, error) {
	return "enc:" + base64.StdEncoding.EncodeToString([]byte(plain)), nil
}

func (dppsTestEncryptor) Decrypt(cipher string) (string, error) {
	b, err := base64.StdEncoding.DecodeString(strings.TrimPrefix(cipher, "enc:"))
	return string(b), err
}

type dppsTestLockCache struct{}

func (dppsTestLockCache) TryAcquireLeaderLock(context.Context, string, string, time.Duration) (bool, error) {
	return true, nil
}

func (dppsTestLockCache) ReleaseLeaderLock(context.Context, string, string) error { return nil }

type dppsTestAccountRepo struct {
	AccountRepository
	accounts map[int64]*Account
}

func (r *dppsTestAccountRepo) GetByID(_ context.Context, id int64) (*Account, error) {
	acc, ok := r.accounts[id]
	if !ok {
		return nil, context.DeadlineExceeded
	}
	return acc, nil
}

func TestDppsResolveChatGPTURL_UnsetAndNonLoopbackAreNoOps(t *testing.T) {
	defaultURLs := []string{
		chatgptCodexURL,
		chatGPTUsageURL,
		chatGPTRateLimitCreditsURL,
		chatGPTRateLimitResetURL,
		chatgptCodexModelsURL,
	}

	t.Setenv(dppsTestChatGPTBaseURLEnv, "")
	for _, u := range defaultURLs {
		require.Equal(t, u, dppsResolveChatGPTURL(u), "unset env must be an exact no-op")
	}

	reqDirect, err := http.NewRequest(http.MethodPost, chatgptCodexURL, nil)
	require.NoError(t, err)
	dppsApplyChatGPTHost(reqDirect)
	require.Equal(t, "chatgpt.com", reqDirect.Host)

	// Even if a request already has a loopback URL, unset env keeps canonical host.
	reqLoopbackWhenUnset, err := http.NewRequest(http.MethodPost, "http://127.0.0.1:18082/backend-api/codex/responses", nil)
	require.NoError(t, err)
	dppsApplyChatGPTHost(reqLoopbackWhenUnset)
	require.Equal(t, "chatgpt.com", reqLoopbackWhenUnset.Host)

	rejectedEnvValues := []string{
		"   ",
		"http://example.com",
		"https://example.com",
		"http://10.0.0.1",
		"http://10.0.0.1:18082",
		"http://169.254.169.254",
		"http://192.168.1.50:18082",
		"http://172.16.0.1:18082",
		"http://0.0.0.0:18082",
		"http://[::]:18082",
		"http://localhost.localdomain:18082",
		"http://sub.localhost:18082",
		"ftp://127.0.0.1:18082",
		"ws://127.0.0.1:18082",
		"http://user:pass@127.0.0.1:18082",
		"http://@127.0.0.1:18082",
		"http://127.0.0.1:18082#frag",
		"http://127.0.0.1:18082#",
		"http://127.0.0.1:18082?foo=bar",
		"http://127.0.0.1:",
		"http://127.0.0.1:99999",
		"not-a-valid-url",
	}

	for _, envVal := range rejectedEnvValues {
		t.Setenv(dppsTestChatGPTBaseURLEnv, envVal)
		for _, u := range defaultURLs {
			require.Equal(t, u, dppsResolveChatGPTURL(u), "rejected override %q must leave %q unchanged", envVal, u)
		}
		req, reqErr := http.NewRequest(http.MethodPost, "http://127.0.0.1:18082/backend-api/codex/responses", nil)
		require.NoError(t, reqErr)
		dppsApplyChatGPTHost(req)
		require.Equal(t, "chatgpt.com", req.Host, "rejected override %q must keep Host chatgpt.com", envVal)
	}
}

func TestDppsResolveChatGPTURL_LoopbackOverridesAndHost(t *testing.T) {
	cases := []struct {
		name     string
		env      string
		in       string
		want     string
		wantHost string
	}{
		{
			name:     "ipv4 loopback",
			env:      "http://127.0.0.1:18082",
			in:       "https://chatgpt.com/backend-api/codex/responses",
			want:     "http://127.0.0.1:18082/backend-api/codex/responses",
			wantHost: "127.0.0.1:18082",
		},
		{
			name:     "ipv4 loopback with trailing slash and query",
			env:      "http://127.0.0.1:18082/",
			in:       "https://chatgpt.com/backend-api/codex/models?client_version=0.100.0",
			want:     "http://127.0.0.1:18082/backend-api/codex/models?client_version=0.100.0",
			wantHost: "127.0.0.1:18082",
		},
		{
			name:     "ipv4 loopback with optional base path",
			env:      "http://127.0.0.1:18082/mock-chatgpt/",
			in:       "https://chatgpt.com/backend-api/wham/usage",
			want:     "http://127.0.0.1:18082/mock-chatgpt/backend-api/wham/usage",
			wantHost: "127.0.0.1:18082",
		},
		{
			name:     "localhost",
			env:      "http://localhost:18082",
			in:       "https://chatgpt.com/backend-api/codex/responses",
			want:     "http://localhost:18082/backend-api/codex/responses",
			wantHost: "localhost:18082",
		},
		{
			name:     "ipv6 loopback",
			env:      "http://[::1]:18082",
			in:       "https://chatgpt.com/backend-api/codex/responses",
			want:     "http://[::1]:18082/backend-api/codex/responses",
			wantHost: "[::1]:18082",
		},
		{
			name:     "https loopback",
			env:      "https://127.0.0.1:18443",
			in:       "https://chatgpt.com/backend-api/codex/responses",
			want:     "https://127.0.0.1:18443/backend-api/codex/responses",
			wantHost: "127.0.0.1:18443",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv(dppsTestChatGPTBaseURLEnv, tc.env)
			got := dppsResolveChatGPTURL(tc.in)
			require.Equal(t, tc.want, got)

			req, err := http.NewRequest(http.MethodPost, got, nil)
			require.NoError(t, err)
			dppsApplyChatGPTHost(req)
			require.Equal(t, tc.wantHost, req.Host)

			// A request that was not overridden still keeps Host chatgpt.com.
			unmodifiedReq, err := http.NewRequest(http.MethodPost, chatgptCodexURL, nil)
			require.NoError(t, err)
			dppsApplyChatGPTHost(unmodifiedReq)
			require.Equal(t, "chatgpt.com", unmodifiedReq.Host)
		})
	}
}

func TestDppsTestHooks_WiredCallSitesAndFakeUpstream(t *testing.T) {
	gin.SetMode(gin.TestMode)

	var (
		mu          sync.Mutex
		seenPaths   []string
		seenHosts   []string
		seenAccount string
	)
	fakeUpstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		seenPaths = append(seenPaths, r.URL.Path)
		seenHosts = append(seenHosts, r.Host)
		seenAccount = r.Header.Get("Chatgpt-Account-Id")
		mu.Unlock()
		switch r.URL.Path {
		case "/backend-api/wham/usage":
			w.Header().Set("Content-Type", "application/json")
			_, _ = io.WriteString(w, `{"plan_type":"plus","rate_limit":{"allowed":true,"limit_reached":false}}`)
		case "/backend-api/wham/rate-limit-reset-credits":
			w.Header().Set("Content-Type", "application/json")
			_, _ = io.WriteString(w, `{"available_count":0,"credits":[]}`)
		case "/backend-api/wham/rate-limit-reset-credits/consume":
			w.Header().Set("Content-Type", "application/json")
			_, _ = io.WriteString(w, `{"code":"ok","windows_reset":1}`)
		case "/backend-api/codex/models":
			w.Header().Set("Content-Type", "application/json")
			_, _ = io.WriteString(w, `{"models":[{"slug":"gpt-5.4","display_name":"GPT-5.4"}]}`)
		default:
			w.Header().Set("Content-Type", "text/event-stream")
			w.Header().Set("x-codex-primary-used-percent", "15")
			w.Header().Set("x-codex-primary-reset-after-seconds", "3600")
			w.Header().Set("x-codex-primary-window-minutes", "10080")
			w.Header().Set("x-codex-secondary-used-percent", "25")
			w.Header().Set("x-codex-secondary-reset-after-seconds", "1800")
			w.Header().Set("x-codex-secondary-window-minutes", "300")
			_, _ = io.WriteString(w, "data: {\"type\":\"response.completed\"}\n\n")
		}
	}))
	defer fakeUpstream.Close()

	parsedFake, err := url.Parse(fakeUpstream.URL)
	require.NoError(t, err)

	gw := &OpenAIGatewayService{}
	acc := &Account{
		ID:       42,
		Platform: PlatformOpenAI,
		Type:     AccountTypeOAuth,
		Credentials: map[string]any{
			"chatgpt_account_id": "acct_dpps_42",
			"access_token":       "tok_fake",
			"expires_at":         "2099-01-01T00:00:00Z",
		},
	}

	// 1. When unset, wired call sites keep chatgpt.com URLs and Host.
	t.Setenv(dppsTestChatGPTBaseURLEnv, "")

	wsDefault, err := gw.buildOpenAIResponsesWSURL(acc)
	require.NoError(t, err)
	require.Equal(t, "wss://chatgpt.com/backend-api/codex/responses", wsDefault)

	wrec := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(wrec)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/responses", nil)
	reqDefault, err := gw.buildUpstreamRequest(context.Background(), c, acc, []byte(`{"model":"gpt-5.4","input":"hi"}`), "tok_fake", true, "", false)
	require.NoError(t, err)
	require.Equal(t, "https://chatgpt.com/backend-api/codex/responses", reqDefault.URL.String())
	require.Equal(t, "chatgpt.com", reqDefault.Host)

	ptDefault, err := gw.buildUpstreamRequestOpenAIPassthrough(context.Background(), c, acc, []byte(`{"model":"gpt-5.4","input":"hi"}`), "tok_fake")
	require.NoError(t, err)
	require.Equal(t, "https://chatgpt.com/backend-api/codex/responses", ptDefault.URL.String())
	require.Equal(t, "chatgpt.com", ptDefault.Host)

	// 2. When set to local fake upstream, wired sites hit http://127.0.0.1:<port> with matching Host.
	t.Setenv(dppsTestChatGPTBaseURLEnv, fakeUpstream.URL)

	wsOverridden, err := gw.buildOpenAIResponsesWSURL(acc)
	require.NoError(t, err)
	require.Equal(t, "ws://"+parsedFake.Host+"/backend-api/codex/responses", wsOverridden)

	reqOverridden, err := gw.buildUpstreamRequest(context.Background(), c, acc, []byte(`{"model":"gpt-5.4","input":"hi"}`), "tok_fake", true, "", false)
	require.NoError(t, err)
	require.Equal(t, fakeUpstream.URL+"/backend-api/codex/responses", reqOverridden.URL.String())
	require.Equal(t, parsedFake.Host, reqOverridden.Host)

	resp, err := fakeUpstream.Client().Do(reqOverridden)
	require.NoError(t, err)
	_ = resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	ptOverridden, err := gw.buildUpstreamRequestOpenAIPassthrough(context.Background(), c, acc, []byte(`{"model":"gpt-5.4","input":"hi"}`), "tok_fake")
	require.NoError(t, err)
	require.Equal(t, fakeUpstream.URL+"/backend-api/codex/responses", ptOverridden.URL.String())
	require.Equal(t, parsedFake.Host, ptOverridden.Host)

	// OpenAIQuotaService hits QueryUsage + rate-limit-reset-credits + consume.
	accRepo := &dppsTestAccountRepo{accounts: map[int64]*Account{42: acc}}
	tokenProvider := NewOpenAITokenProvider(accRepo, nil, nil)
	quotaSvc := NewOpenAIQuotaService(accRepo, nil, tokenProvider, func(string) (*req.Client, error) {
		return req.C(), nil
	}, nil)
	quotaUsage, err := quotaSvc.QueryUsage(context.Background(), 42)
	require.NoError(t, err)
	require.Equal(t, "plus", quotaUsage.PlanType)

	resetRes, err := quotaSvc.ResetCredit(context.Background(), 42)
	require.NoError(t, err)
	require.Equal(t, "ok", resetRes.Code)

	// FetchCodexModelsManifest hits /backend-api/codex/models on the fake upstream.
	manifest, err := gw.FetchCodexModelsManifest(context.Background(), acc, "", "")
	require.NoError(t, err)
	require.Contains(t, string(manifest.Body), "gpt-5.4")

	// AccountUsageService probe hits /backend-api/codex/responses on the fake upstream.
	usageSvc := &AccountUsageService{}
	probeUpdates, err := usageSvc.probeOpenAICodexSnapshot(context.Background(), acc)
	require.NoError(t, err)
	require.EqualValues(t, 25, probeUpdates["codex_5h_used_percent"])
	require.EqualValues(t, 15, probeUpdates["codex_7d_used_percent"])

	mu.Lock()
	defer mu.Unlock()
	require.Equal(t, []string{
		"/backend-api/codex/responses",
		"/backend-api/wham/usage",
		"/backend-api/wham/rate-limit-reset-credits",
		"/backend-api/wham/rate-limit-reset-credits/consume",
		"/backend-api/codex/models",
		"/backend-api/codex/responses",
	}, seenPaths)
	for _, host := range seenHosts {
		require.Equal(t, parsedFake.Host, host)
	}
	require.Equal(t, "acct_dpps_42", seenAccount)
}

func TestDppsLoopbackWebhookHook_EnvValidationDialerAndDelivery(t *testing.T) {
	var (
		mu           sync.Mutex
		receivedBody []byte
		receivedSig  string
		receivedID   string
	)
	receiver := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		mu.Lock()
		receivedBody = body
		receivedSig = r.Header.Get(DppsEventSignatureHeader)
		receivedID = r.Header.Get(DppsEventIDHeader)
		mu.Unlock()
		w.WriteHeader(http.StatusOK)
	}))
	defer receiver.Close()

	// 1. Unset (and non-"true") leaves production SSRF rules untouched.
	for _, envVal := range []string{"", "false", "1", "yes"} {
		t.Setenv(dppsTestAllowLoopbackWebhookEnv, envVal)
		require.ErrorIs(t, dppsValidateEventsWebhookURL(receiver.URL+"/events"), ErrDppsEventsWebhookURLInvalid)
		require.ErrorIs(t, dppsValidateEventsWebhookURL("https://127.0.0.1:8443/events"), ErrDppsEventsWebhookURLInvalid)
		require.ErrorIs(t, dppsValidateEventsWebhookURL("http://localhost:18080/events"), ErrDppsEventsWebhookURLInvalid)
		require.ErrorIs(t, dppsValidateEventsWebhookURL("http://example.com/events"), ErrDppsEventsWebhookURLInvalid)
		require.ErrorIs(t, dppsValidateEventsWebhookURL("http://10.0.0.1/events"), ErrDppsEventsWebhookURLInvalid)
		require.ErrorIs(t, dppsValidateEventsWebhookURL("http://169.254.169.254/latest"), ErrDppsEventsWebhookURLInvalid)
		require.NoError(t, dppsValidateEventsWebhookURL("https://93.184.216.34/events"))

		u, err := url.Parse(receiver.URL)
		require.NoError(t, err)
		conn, dialErr := dppsEventsWebhookDialContext(context.Background(), "tcp", u.Host)
		require.Error(t, dialErr, "loopback dial must remain blocked when env=%q", envVal)
		if conn != nil {
			_ = conn.Close()
		}
	}

	// 2. When SUB2API_TEST_DPPS_ALLOW_LOOPBACK_WEBHOOK=true:
	//    - loopback http/https URLs are allowed
	//    - non-loopback HTTP, private LAN IPs, link-local metadata, userinfo, fragments stay rejected
	//    - loopback-only DialContext still rejects private/public non-loopback addresses
	t.Setenv(dppsTestAllowLoopbackWebhookEnv, "true")

	require.NoError(t, dppsValidateEventsWebhookURL(receiver.URL+"/events"))
	require.NoError(t, dppsValidateEventsWebhookURL("https://127.0.0.1:8443/events"))
	require.NoError(t, dppsValidateEventsWebhookURL("http://localhost:18080/events"))
	require.NoError(t, dppsValidateEventsWebhookURL("http://[::1]:18080/events"))

	rejectedURLs := []string{
		"http://example.com/events",
		"http://10.0.0.1/events",
		"https://10.0.0.1/events",
		"http://169.254.169.254/latest/meta-data",
		"https://169.254.169.254/latest/meta-data",
		"http://192.168.1.1/events",
		"https://192.168.1.1/events",
		"http://172.16.0.1/events",
		"http://0.0.0.0:18080/events",
		"ftp://127.0.0.1:18080/events",
		"http://user@127.0.0.1:18080/events",
		"http://127.0.0.1:18080/events#frag",
		"http://127.0.0.1:18080/events#",
	}
	for _, u := range rejectedURLs {
		require.ErrorIs(t, dppsValidateEventsWebhookURL(u), ErrDppsEventsWebhookURLInvalid, "non-loopback/malformed %q must still be rejected", u)
	}

	for _, addr := range []string{"10.0.0.1:80", "169.254.169.254:80", "192.168.1.1:443", "93.184.216.34:443", "example.com:80"} {
		conn, dialErr := dppsEventsWebhookDialContext(context.Background(), "tcp", addr)
		require.Error(t, dialErr, "loopback-only DialContext must reject %q", addr)
		if conn != nil {
			_ = conn.Close()
		}
	}

	// 3. Full DppsEventsService flow with allowInsecure=false and default httpClient!
	repo := &dppsTestSettingRepo{data: map[string]string{}}
	outbox := newDppsTestOutboxRepo()
	svc := NewDppsEventsService(repo, outbox, dppsTestEncryptor{}, func() bool { return true }, nil, dppsTestLockCache{}, nil, "0.2.23")
	require.False(t, svc.allowInsecure, "production default allowInsecure must stay false")

	ctx := context.Background()
	_, err := svc.Update(ctx, DppsEventsWebhookConfig{
		Enabled:    true,
		URL:        receiver.URL + "/events",
		Secret:     "hook-secret",
		EventTypes: []string{DppsEventSeatCreated},
	})
	require.NoError(t, err)

	svc.Emit(ctx, DppsEventSeatCreated, map[string]any{"account_id": int64(7), "platform": "openai"})
	svc.RunOnce(ctx)

	mu.Lock()
	bodyCopy := append([]byte(nil), receivedBody...)
	sigCopy := receivedSig
	idCopy := receivedID
	mu.Unlock()

	require.NotEmpty(t, bodyCopy, "loopback webhook should have delivered event")
	require.NotEmpty(t, idCopy)
	require.True(t, DppsVerifyEventSignature("hook-secret", sigCopy, bodyCopy, time.Now(), 5*time.Minute))

	var envelope map[string]any
	require.NoError(t, json.Unmarshal(bodyCopy, &envelope))
	require.Equal(t, DppsEventSeatCreated, envelope["type"])
	require.Equal(t, idCopy, envelope["id"])

	// Unsetting the env var mid-run immediately blocks delivery again.
	t.Setenv(dppsTestAllowLoopbackWebhookEnv, "")
	errClass := svc.send(ctx, &DppsEventsWebhookConfig{URL: receiver.URL + "/events", Secret: "hook-secret"}, DppsEventOutboxRecord{
		ID: "ev-2", Type: DppsEventSeatCreated, Body: []byte(`{"id":"ev-2"}`),
	})
	require.Equal(t, "url_blocked", errClass)
}
