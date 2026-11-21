//go:build unit

package service

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"regexp"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// ---- fakes -------------------------------------------------------------------

type evtOutbox struct {
	mu   sync.Mutex
	recs map[string]*evtOutboxRow
	ord  []string
}

type evtOutboxRow struct {
	EventOutboxRecord
	Status    string
	LastError string
}

func newEvtOutbox() *evtOutbox { return &evtOutbox{recs: map[string]*evtOutboxRow{}} }

func (o *evtOutbox) Insert(_ context.Context, rec EventOutboxRecord) error {
	o.mu.Lock()
	defer o.mu.Unlock()
	if _, dup := o.recs[rec.ID]; dup {
		return nil // like the real table: ON CONFLICT (id) DO NOTHING
	}
	o.recs[rec.ID] = &evtOutboxRow{EventOutboxRecord: rec, Status: "pending"}
	o.ord = append(o.ord, rec.ID)
	return nil
}

func (o *evtOutbox) ListDue(_ context.Context, now time.Time, limit int) ([]EventOutboxRecord, error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	var out []EventOutboxRecord
	for _, id := range o.ord {
		r := o.recs[id]
		if r.Status == "pending" && !r.NextAttemptAt.After(now) && len(out) < limit {
			out = append(out, r.EventOutboxRecord)
		}
	}
	return out, nil
}

func (o *evtOutbox) MarkSent(_ context.Context, id string, _ time.Time) error {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.recs[id].Status = "sent"
	o.recs[id].Attempts++
	return nil
}

func (o *evtOutbox) Reschedule(_ context.Context, id string, attempts int, next time.Time, errClass string) error {
	o.mu.Lock()
	defer o.mu.Unlock()
	r := o.recs[id]
	r.Attempts, r.NextAttemptAt, r.LastError = attempts, next, errClass
	return nil
}

func (o *evtOutbox) MarkFailed(_ context.Context, id string, attempts int, errClass string, _ time.Time) error {
	o.mu.Lock()
	defer o.mu.Unlock()
	r := o.recs[id]
	r.Status, r.Attempts, r.LastError = "failed", attempts, errClass
	return nil
}

func (o *evtOutbox) Prune(context.Context, time.Time) (int64, error) { return 0, nil }

func (o *evtOutbox) types() []string {
	o.mu.Lock()
	defer o.mu.Unlock()
	var out []string
	for _, id := range o.ord {
		out = append(out, o.recs[id].Type)
	}
	return out
}

func (o *evtOutbox) bodies(eventType string) []map[string]any {
	o.mu.Lock()
	defer o.mu.Unlock()
	var out []map[string]any
	for _, id := range o.ord {
		if o.recs[id].Type != eventType {
			continue
		}
		var m map[string]any
		_ = json.Unmarshal(o.recs[id].Body, &m)
		out = append(out, m)
	}
	return out
}

func (o *evtOutbox) status(id string) string {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.recs[id].Status
}

type evtEncryptor struct{}

func (evtEncryptor) Encrypt(p string) (string, error) {
	return "enc:" + base64.StdEncoding.EncodeToString([]byte(p)), nil
}

func (evtEncryptor) Decrypt(c string) (string, error) {
	b, err := base64.StdEncoding.DecodeString(strings.TrimPrefix(c, "enc:"))
	return string(b), err
}

type evtLockCache struct {
	mu    sync.Mutex
	owner map[string]string
}

func (l *evtLockCache) TryAcquireLeaderLock(_ context.Context, key, owner string, _ time.Duration) (bool, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.owner == nil {
		l.owner = map[string]string{}
	}
	if _, held := l.owner[key]; held {
		return false, nil
	}
	l.owner[key] = owner
	return true, nil
}

func (l *evtLockCache) ReleaseLeaderLock(_ context.Context, key, owner string) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.owner[key] == owner {
		delete(l.owner, key)
	}
	return nil
}

type evtData struct {
	mu       sync.Mutex
	groups   []EventGroupModels
	seats    []map[string]any
	balances map[int64]float64
}

func (d *evtData) AccountBrief(_ context.Context, id int64) (map[string]any, bool) {
	return map[string]any{"account_name": "seat-" + strings.Repeat("x", int(id)), "platform": "openai"}, true
}

func (d *evtData) GroupModelLists(context.Context) ([]EventGroupModels, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	return append([]EventGroupModels(nil), d.groups...), nil
}

func (d *evtData) SeatSnapshots(context.Context) ([]map[string]any, error) { return d.seats, nil }
func (d *evtData) UserBalance(_ context.Context, id int64) (float64, bool) {
	b, ok := d.balances[id]
	return b, ok
}
func (d *evtData) PriceTableHash() string { return "pricehash" }

func (d *evtData) setGroups(g ...EventGroupModels) {
	d.mu.Lock()
	d.groups = g
	d.mu.Unlock()
}

// evtSink is a fake EventSink that records everything handed to it.
type evtSink struct {
	mu      sync.Mutex
	events  []evtRecorded
	watched map[int64]float64
	models  int
}

type evtRecorded struct {
	Type string
	Data map[string]any
}

func (s *evtSink) Wants(string) bool { return true }
func (s *evtSink) Emit(_ context.Context, t string, d map[string]any) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.events = append(s.events, evtRecorded{t, d})
}
func (s *evtSink) BalanceWatch(id int64) (float64, bool) { th, ok := s.watched[id]; return th, ok }
func (s *evtSink) ScheduleModelsCheck()                  { s.mu.Lock(); s.models++; s.mu.Unlock() }
func (s *evtSink) byType(t string) []evtRecorded {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []evtRecorded
	for _, e := range s.events {
		if e.Type == t {
			out = append(out, e)
		}
	}
	return out
}

func installEvtSink(t *testing.T, s EventSink) {
	t.Helper()
	SetEventSink(s)
	t.Cleanup(func() { SetEventSink(nil) })
}

// ---- harness -----------------------------------------------------------------

type evtHarness struct {
	svc    *EventsService
	outbox *evtOutbox
	repo   *mockSettingRepo
	data   *evtData
	clock  *time.Time
	srv    *httptest.Server
	mu     sync.Mutex
	hits   []evtHit
	status func(n int) int
}

type evtHit struct {
	ID, Sig string
	Body    []byte
}

func newEvtHarness(t *testing.T, lock LeaderLockCache, outbox *evtOutbox, repo *mockSettingRepo) *evtHarness {
	t.Helper()
	h := &evtHarness{outbox: outbox, repo: repo, data: &evtData{balances: map[int64]float64{}}}
	if h.outbox == nil {
		h.outbox = newEvtOutbox()
	}
	if h.repo == nil {
		h.repo = newMockSettingRepo()
	}
	clock := time.Date(2026, 11, 20, 12, 0, 0, 0, time.UTC)
	h.clock = &clock
	h.status = func(int) int { return 200 }
	h.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body := make([]byte, 1<<20)
		n, _ := r.Body.Read(body)
		h.mu.Lock()
		h.hits = append(h.hits, evtHit{ID: r.Header.Get(EventIDHeader), Sig: r.Header.Get(EventSignatureHeader), Body: body[:n]})
		n2 := len(h.hits)
		h.mu.Unlock()
		time.Sleep(20 * time.Millisecond)
		w.WriteHeader(h.status(n2))
	}))
	t.Cleanup(h.srv.Close)
	h.svc = NewEventsService(h.repo, h.outbox, evtEncryptor{}, func() bool { return true }, h.data, lock, nil, "0.2.23", "")
	h.svc.allowInsecure = true
	h.svc.httpClient = h.srv.Client()
	h.svc.now = func() time.Time { return *h.clock }
	return h
}

func (h *evtHarness) configure(t *testing.T, mutate func(*EventsWebhookConfig)) {
	t.Helper()
	cfg := EventsWebhookConfig{Enabled: true, URL: h.srv.URL + "/events", Secret: "s3cret-value"}
	if mutate != nil {
		mutate(&cfg)
	}
	_, err := h.svc.Update(context.Background(), cfg)
	require.NoError(t, err)
}

func (h *evtHarness) hitCount() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return len(h.hits)
}

func onlySeatStatus(c *EventsWebhookConfig) { c.EventTypes = []string{EventSeatStatus} }

// ---- signature ---------------------------------------------------------------

func TestSignEventBody_FormatAndVerification(t *testing.T) {
	now := time.Unix(1_800_000_000, 0)
	body := []byte(`{"id":"1"}`)
	sig := SignEventBody("secret", now, body)

	require.Regexp(t, regexp.MustCompile(`^t=1800000000,v1=[0-9a-f]{64}$`), sig)
	require.True(t, VerifyEventSignature("secret", sig, body, now.Add(time.Minute), 5*time.Minute))
	require.False(t, VerifyEventSignature("other", sig, body, now, 5*time.Minute), "wrong secret")
	require.False(t, VerifyEventSignature("secret", sig, []byte(`{"id":"2"}`), now, 5*time.Minute), "tampered body")
	require.False(t, VerifyEventSignature("secret", sig, body, now.Add(6*time.Minute), 5*time.Minute), "older than 5 minutes")
	require.False(t, VerifyEventSignature("secret", "v1=abc", body, now, 5*time.Minute), "no timestamp")
}

// ---- configuration -----------------------------------------------------------

func TestEventsService_OffByDefaultAndSecretEncryptedAtRest(t *testing.T) {
	h := newEvtHarness(t, nil, nil, nil)
	require.False(t, h.svc.Wants(EventSeatStatus), "off until an admin enables it")

	h.configure(t, func(c *EventsWebhookConfig) { c.EventTypes = []string{EventSeatStatus} })
	require.True(t, h.svc.Wants(EventSeatStatus))
	require.False(t, h.svc.Wants(EventSeatUsage), "only the chosen types")

	stored := h.repo.data[settingKeyEventsWebhookConfig]
	require.NotContains(t, stored, "s3cret-value", "plaintext secret must not be stored")
	require.Contains(t, stored, "enc:")

	cfg, configured, err := h.svc.Get(context.Background())
	require.NoError(t, err)
	require.True(t, configured)
	require.Empty(t, cfg.Secret, "Get never returns the secret")

	// An empty secret on save keeps the stored one.
	_, err = h.svc.Update(context.Background(), EventsWebhookConfig{Enabled: true, URL: h.srv.URL + "/events", EventTypes: []string{EventSeatStatus}})
	require.NoError(t, err)
	require.Contains(t, h.repo.data[settingKeyEventsWebhookConfig], "enc:czNjcmV0LXZhbHVl")
}

func TestEventsService_UpdateValidation(t *testing.T) {
	h := newEvtHarness(t, nil, nil, nil)
	h.svc.allowInsecure = false

	_, err := h.svc.Update(context.Background(), EventsWebhookConfig{Enabled: true, URL: "http://example.com/e", Secret: "x"})
	require.ErrorIs(t, err, ErrEventsWebhookURLInvalid, "https only")
	_, err = h.svc.Update(context.Background(), EventsWebhookConfig{Enabled: true, URL: "https://127.0.0.1/e", Secret: "x"})
	require.ErrorIs(t, err, ErrEventsWebhookURLInvalid, "no loopback")
	_, err = h.svc.Update(context.Background(), EventsWebhookConfig{Enabled: true, URL: "https://10.1.2.3/e", Secret: "x"})
	require.ErrorIs(t, err, ErrEventsWebhookURLInvalid, "no private address")
	_, err = h.svc.Update(context.Background(), EventsWebhookConfig{URL: "https://93.184.216.34/e", Secret: "x", SnapshotTimeUTC: "25:99"})
	require.ErrorIs(t, err, ErrEventsSnapshotTime)
	_, err = h.svc.Update(context.Background(), EventsWebhookConfig{URL: "https://93.184.216.34/e", Secret: "x", EventTypes: []string{"nope.type"}})
	require.ErrorIs(t, err, ErrEventsUnknownType)
	_, err = h.svc.Update(context.Background(), EventsWebhookConfig{Enabled: true, URL: "https://93.184.216.34/e"})
	require.Error(t, err, "enabled needs a secret")
	_, err = h.svc.Update(context.Background(), EventsWebhookConfig{Enabled: true, URL: "https://93.184.216.34/e", Secret: "x", SnapshotTimeUTC: "03:30"})
	require.NoError(t, err)
}

// ---- delivery ----------------------------------------------------------------

func TestEventsService_DeliversSignedAndOutboxSurvivesFailure(t *testing.T) {
	h := newEvtHarness(t, nil, nil, nil)
	h.configure(t, onlySeatStatus)
	h.status = func(n int) int {
		if n == 1 {
			return 500
		}
		return 200
	}
	ctx := context.Background()
	h.svc.Emit(ctx, EventSeatStatus, map[string]any{"account_id": int64(2), "status": "error", "reason": "auth"})
	require.Len(t, h.outbox.types(), 1)
	id := h.outbox.ord[0]

	h.svc.RunOnce(ctx)
	require.Equal(t, 1, h.hitCount())
	require.Equal(t, "pending", h.outbox.status(id), "a failure keeps the event")
	require.Equal(t, 1, h.outbox.recs[id].Attempts)
	require.Equal(t, "http_500", h.outbox.recs[id].LastError)
	require.Equal(t, h.clock.Add(10*time.Second), h.outbox.recs[id].NextAttemptAt, "first retry after 10 s")

	h.svc.RunOnce(ctx) // too early: nothing is sent
	require.Equal(t, 1, h.hitCount())

	*h.clock = h.clock.Add(11 * time.Second)
	h.svc.RunOnce(ctx)
	require.Equal(t, 2, h.hitCount())
	require.Equal(t, "sent", h.outbox.status(id))

	for _, hit := range h.hits {
		require.Equal(t, id, hit.ID, "same event id on every retry")
		require.True(t, VerifyEventSignature("s3cret-value", hit.Sig, hit.Body, *h.clock, 5*time.Minute))
	}
	var env map[string]any
	require.NoError(t, json.Unmarshal(h.hits[1].Body, &env))
	require.Equal(t, id, env["id"])
	require.Equal(t, EventSeatStatus, env["type"])
	require.Equal(t, "0.2.23", env["sub2api_version"])
	require.Equal(t, "seat-xx", env["data"].(map[string]any)["account_name"], "name added from the data source")
}

func TestEventBackoffAndDeadline(t *testing.T) {
	want := []time.Duration{10 * time.Second, 20 * time.Second, 40 * time.Second, 80 * time.Second}
	for i, w := range want {
		require.Equal(t, w, eventBackoff(i+1))
	}
	require.Equal(t, time.Hour, eventBackoff(12), "capped at one hour")
	require.Equal(t, time.Hour, eventBackoff(40))

	h := newEvtHarness(t, nil, nil, nil)
	h.configure(t, onlySeatStatus)
	h.status = func(int) int { return 503 }
	ctx := context.Background()
	h.svc.Emit(ctx, EventSeatStatus, map[string]any{"account_id": int64(1), "status": "error"})
	id := h.outbox.ord[0]
	// keep failing for 24 hours of retries
	for i := 0; i < 200 && h.outbox.status(id) == "pending"; i++ {
		h.svc.RunOnce(ctx)
		*h.clock = h.outbox.recs[id].NextAttemptAt
		if h.clock.Sub(h.outbox.recs[id].CreatedAt) > 25*time.Hour {
			break
		}
	}
	require.Equal(t, "failed", h.outbox.status(id), "marked failed after 24 h")
	hitsAtFail := h.hitCount()
	h.svc.RunOnce(ctx)
	require.Equal(t, hitsAtFail, h.hitCount(), "a failed event is never sent again")
}

func TestEventsService_LeaderLockMeansNoDoubleSend(t *testing.T) {
	outbox := newEvtOutbox()
	repo := newMockSettingRepo()
	lock := &evtLockCache{}
	a := newEvtHarness(t, lock, outbox, repo)
	b := newEvtHarness(t, lock, outbox, repo)
	b.srv.Close()
	b.svc.httpClient = a.srv.Client()
	a.configure(t, onlySeatStatus)
	b.svc.invalidateConfig()
	b.svc.now = a.svc.now
	// b posts to a's server through the shared stored URL
	ctx := context.Background()
	for i := 0; i < 3; i++ {
		a.svc.Emit(ctx, EventSeatStatus, map[string]any{"account_id": int64(i + 1), "status": "error"})
	}

	var wg sync.WaitGroup
	for _, s := range []*EventsService{a.svc, b.svc} {
		wg.Add(1)
		go func(s *EventsService) { defer wg.Done(); s.RunOnce(ctx) }(s)
	}
	wg.Wait()

	seen := map[string]int{}
	a.mu.Lock()
	for _, hit := range a.hits {
		seen[hit.ID]++
	}
	a.mu.Unlock()
	require.Len(t, seen, 3)
	for id, n := range seen {
		require.Equal(t, 1, n, "event %s sent once", id)
	}

	// A lock held by a peer means this replica sends nothing.
	_, _ = lock.TryAcquireLeaderLock(ctx, eventsLeaderLockKey, "peer", time.Minute)
	a.svc.Emit(ctx, EventSeatStatus, map[string]any{"account_id": int64(9), "status": "error"})
	before := a.hitCount()
	a.svc.RunOnce(ctx)
	require.Equal(t, before, a.hitCount())
}

// ---- seat.limited merge ------------------------------------------------------

func TestEventsService_SeatLimitedMergedWithinAMinute(t *testing.T) {
	h := newEvtHarness(t, nil, nil, nil)
	h.configure(t, nil)
	ctx := context.Background()
	d := func() map[string]any {
		return map[string]any{"account_id": int64(4), "kind": "rate_limit", "until": "2026-11-20T13:00:00Z"}
	}
	h.svc.Emit(ctx, EventSeatLimited, d())
	h.svc.Emit(ctx, EventSeatLimited, d())
	*h.clock = h.clock.Add(30 * time.Second)
	h.svc.Emit(ctx, EventSeatLimited, d())
	require.Len(t, h.outbox.bodies(EventSeatLimited), 1)
	other := d()
	other["kind"] = "overload"
	h.svc.Emit(ctx, EventSeatLimited, other)
	require.Len(t, h.outbox.bodies(EventSeatLimited), 2, "another kind is its own event")
	*h.clock = h.clock.Add(2 * time.Minute)
	h.svc.Emit(ctx, EventSeatLimited, d())
	require.Len(t, h.outbox.bodies(EventSeatLimited), 3)
}

// ---- models.changed -----------------------------------------------------------

func TestEventsService_ModelsChangedOnlyWhenHashChanges(t *testing.T) {
	h := newEvtHarness(t, nil, nil, nil)
	h.configure(t, nil)
	ctx := context.Background()
	h.data.setGroups(EventGroupModels{GroupID: 1, Name: "g1", Models: []string{"b", "a"}})

	h.svc.CheckModels(ctx)
	first := h.outbox.bodies(EventModelsChanged)
	require.Len(t, first, 1, "a first look reports the whole list as added")
	d := first[0]["data"].(map[string]any)
	require.Equal(t, []any{"a", "b"}, d["added"])
	require.Equal(t, []any{}, d["removed"])
	require.Equal(t, []any{"a", "b"}, d["models"])

	h.data.setGroups(EventGroupModels{GroupID: 1, Name: "g1", Models: []string{"a", "b", "a"}}) // same set, other order
	h.svc.CheckModels(ctx)
	require.Len(t, h.outbox.bodies(EventModelsChanged), 1, "same set: no event")

	h.data.setGroups(EventGroupModels{GroupID: 1, Name: "g1", Models: []string{"a", "c"}})
	h.svc.CheckModels(ctx)
	got := h.outbox.bodies(EventModelsChanged)
	require.Len(t, got, 2)
	d = got[1]["data"].(map[string]any)
	require.Equal(t, []any{"c"}, d["added"])
	require.Equal(t, []any{"b"}, d["removed"])
	require.EqualValues(t, 1, d["group_id"])

	h.svc.CheckModels(ctx)
	require.Len(t, h.outbox.bodies(EventModelsChanged), 2, "no change since: still two")
}

// ---- system and snapshot -------------------------------------------------------

func TestEventsService_SystemStartedAndVersionChanged(t *testing.T) {
	repo := newMockSettingRepo()
	h := newEvtHarness(t, nil, nil, repo)
	h.configure(t, nil)
	ctx := context.Background()
	h.svc.RunOnce(ctx)
	h.svc.RunOnce(ctx)
	require.Len(t, h.outbox.bodies(EventSystemStarted), 1, "once per start")
	require.Empty(t, h.outbox.bodies(EventVersionChanged), "no earlier version known")

	h2 := newEvtHarness(t, nil, nil, repo) // a restart on a newer version, same stored state
	h2.svc.version = "0.2.24"
	h2.svc.invalidateConfig()
	h2.svc.RunOnce(ctx)
	require.Len(t, h2.outbox.bodies(EventSystemStarted), 1)
	vc := h2.outbox.bodies(EventVersionChanged)
	require.Len(t, vc, 1)
	require.Equal(t, "0.2.23", vc[0]["data"].(map[string]any)["previous_version"])
}

func TestEventsService_DailySnapshotOncePerDayAtSetTime(t *testing.T) {
	h := newEvtHarness(t, nil, nil, nil)
	h.configure(t, func(c *EventsWebhookConfig) {
		c.SnapshotTimeUTC = "13:00"
		c.BalanceUserIDs = []int64{7}
	})
	h.data.seats = []map[string]any{{"account_id": 1, "status": "active"}}
	h.data.balances[7] = 12.5
	h.data.setGroups(EventGroupModels{GroupID: 1, Name: "g", Models: []string{"m"}})
	ctx := context.Background()

	h.svc.RunOnce(ctx) // 12:00, before the set time
	require.Empty(t, h.outbox.bodies(EventSnapshotDaily))
	*h.clock = time.Date(2026, 11, 20, 13, 0, 5, 0, time.UTC)
	h.svc.RunOnce(ctx)
	h.svc.RunOnce(ctx)
	snaps := h.outbox.bodies(EventSnapshotDaily)
	require.Len(t, snaps, 1)
	d := snaps[0]["data"].(map[string]any)
	require.Equal(t, "pricehash", d["price_table_hash"])
	require.Len(t, d["seats"], 1)
	require.Len(t, d["groups"], 1)
	require.Equal(t, 12.5, d["balances"].([]any)[0].(map[string]any)["balance"])
	*h.clock = h.clock.Add(24 * time.Hour)
	h.svc.RunOnce(ctx)
	require.Len(t, h.outbox.bodies(EventSnapshotDaily), 2, "the next day gets its own")
}

// ---- nothing secret in any event ------------------------------------------------

func TestEventPayloads_NeverCarrySecretLookingFields(t *testing.T) {
	h := newEvtHarness(t, nil, nil, nil)
	h.configure(t, nil)
	ctx := context.Background()

	// Hostile input: a careless emit point must still not leak.
	h.svc.Emit(ctx, EventSeatUpdated, map[string]any{
		"account_id": int64(3), "access_token": "tok", "refresh_token": "r", "Password": "p", "client_secret": "s",
		"credentials": map[string]any{"api_key": "sk-1"}, "api_key": "sk-2", "owner": "someone@example.com",
		"changed_fields": []string{"credentials"}, "note": "login as bob@corp.io now",
		"nested": map[string]any{"authorization": "Bearer x", "ok": "fine"},
	})
	body := h.outbox.bodies(EventSeatUpdated)[0]
	raw, _ := json.Marshal(body)
	text := strings.ToLower(string(raw))
	for _, bad := range []string{"token", "password", "secret", "sk-1", "sk-2", "bearer", "@", "api_key"} {
		require.NotContains(t, text, bad)
	}
	d := body["data"].(map[string]any)
	require.Equal(t, "fine", d["nested"].(map[string]any)["ok"])
	require.Equal(t, "login as [email] now", d["note"])
	require.Equal(t, []any{"credentials"}, d["changed_fields"], "field names are allowed, values are not")

	require.Equal(t, "auth", SeatErrorReasonClass("HTTP 401 Unauthorized: token abc"))
	require.Equal(t, "refresh_failed", SeatErrorReasonClass("token refresh failed: boom"))
	require.Equal(t, "forbidden", SeatErrorReasonClass("403 forbidden"))
	require.Equal(t, "upstream_code", SeatErrorReasonClass("upstream returned 502"))
	require.Equal(t, "other", SeatErrorReasonClass("weird"))
}

// ---- emit points with a fake sink --------------------------------------------------

func TestSeatUsage_OnlyOnCrossingsAndWindowReset(t *testing.T) {
	sink := &evtSink{}
	installEvtSink(t, sink)
	ctx := context.Background()
	id := int64(41)
	upd := func(p5, p7 float64, reset5 string) map[string]any {
		return map[string]any{
			"codex_5h_used_percent": p5, "codex_5h_reset_at": reset5,
			"codex_7d_used_percent": p7, "codex_7d_reset_at": "2026-11-27T00:00:00Z",
		}
	}
	r1 := "2026-11-20T15:00:00Z"
	EmitSeatUsageFromExtra(ctx, id, upd(10, 5, r1)) // first reading seeds only
	EmitSeatUsageFromExtra(ctx, id, upd(30, 5, r1)) // no level crossed
	EmitSeatUsageFromExtra(ctx, id, upd(49.9, 5, r1))
	require.Empty(t, sink.byType(EventSeatUsage), "no crossings yet")

	EmitSeatUsageFromExtra(ctx, id, upd(52, 5, r1)) // crosses 50
	require.Len(t, sink.byType(EventSeatUsage), 1)
	EmitSeatUsageFromExtra(ctx, id, upd(60, 5, r1)) // same level
	require.Len(t, sink.byType(EventSeatUsage), 1)
	EmitSeatUsageFromExtra(ctx, id, upd(71, 5, r1)) // 70
	EmitSeatUsageFromExtra(ctx, id, upd(95, 5, r1)) // 90
	EmitSeatUsageFromExtra(ctx, id, upd(100, 5, r1))
	require.Len(t, sink.byType(EventSeatUsage), 4)

	last := sink.byType(EventSeatUsage)[3].Data
	require.EqualValues(t, id, last["account_id"])
	require.EqualValues(t, 100, last["used_percent_5h"])
	crossed := last["crossed"].([]map[string]any)
	require.Equal(t, "5h", crossed[0]["window"])
	require.Equal(t, 100, crossed[0]["level"])
	require.Equal(t, "crossing", crossed[0]["reason"])

	EmitSeatUsageFromExtra(ctx, id, upd(2, 5, "2026-11-20T20:00:00Z")) // window reset
	ev := sink.byType(EventSeatUsage)
	require.Len(t, ev, 5)
	require.Equal(t, "reset", ev[4].Data["crossed"].([]map[string]any)[0]["reason"])
	EmitSeatUsageFromExtra(ctx, id, upd(2.5, 5, "2026-11-20T20:00:03Z"))
	require.Len(t, sink.byType(EventSeatUsage), 5, "a few seconds of reset_at jitter is not a reset")
}

func TestEmitHelpers_FireTheRightEventOnce(t *testing.T) {
	sink := &evtSink{watched: map[int64]float64{7: 5}}
	installEvtSink(t, sink)
	ctx := context.Background()

	EmitSeatStatus(ctx, 1, StatusError, "auth")
	EmitSeatSchedulable(ctx, 1, false, "admin")
	EmitSeatSchedulable(ctx, 1, true, "expiry")
	EmitSeatLimited(ctx, 1, "rate_limit", time.Date(2026, 11, 20, 13, 0, 0, 0, time.UTC), "5h")
	EmitSeatCleared(ctx, 1, "rate_limit")
	prio := 3
	EmitSeatLifecycle(ctx, EventSeatCreated, 1, "seat-one", "openai", &prio, nil)
	EmitChangedEntity(ctx, EventGroupChanged, 5, "g", "updated", []string{"rate_multiplier"}, nil)
	EmitChangedEntity(ctx, EventChannelChanged, 6, "c", "deleted", nil, nil)
	EmitKeyChanged(ctx, 7, 70, "dpps-key", StatusAPIKeyQuotaExhausted, nil)
	EmitKeyChanged(ctx, 8, 80, "someone-else", StatusAPIKeyQuotaExhausted, nil) // not a watched user
	EmitSettingsChanged(ctx, []string{"smtp_host", "site_name"})
	EmitAlert(ctx, true, 3, "high error rate", "P1", "error_rate", f64(12), f64(5), map[string]any{"group_id": int64(2)})
	EmitAlert(ctx, false, 3, "high error rate", "P1", "error_rate", f64(1), f64(5), nil)

	want := map[string]int{
		EventSeatStatus: 1, EventSeatPaused: 1, EventSeatResumed: 1, EventSeatLimited: 1, EventSeatCleared: 1,
		EventSeatCreated: 1, EventGroupChanged: 1, EventChannelChanged: 1, EventKeyChanged: 1,
		EventSettingsChanged: 1, EventAlertFired: 1, EventAlertResolved: 1,
	}
	for typ, n := range want {
		require.Len(t, sink.byType(typ), n, typ)
	}
	require.Equal(t, 3, sink.models, "seat, group and channel edits schedule a models check")
	require.Equal(t, "expiry", sink.byType(EventSeatResumed)[0].Data["by"])
	require.Equal(t, "2026-11-20T13:00:00Z", sink.byType(EventSeatLimited)[0].Data["until"])
	require.Equal(t, []string{"site_name", "smtp_host"}, sink.byType(EventSettingsChanged)[0].Data["changed_keys"])
	require.EqualValues(t, 2, sink.byType(EventAlertFired)[0].Data["group"])
}

func f64(v float64) *float64 { return &v }

func TestBalanceEvents_LowOnCrossingAndChangedForWatchedUsers(t *testing.T) {
	sink := &evtSink{watched: map[int64]float64{7: 5}}
	installEvtSink(t, sink)
	ctx := context.Background()

	EmitBalanceLowIfCrossed(ctx, 7, 9, 6) // still above
	EmitBalanceLowIfCrossed(ctx, 7, 6, 4) // crosses
	EmitBalanceLowIfCrossed(ctx, 7, 4, 3) // already below
	EmitBalanceLowIfCrossed(ctx, 8, 9, 1) // not watched
	require.Len(t, sink.byType(EventBalanceLow), 1)
	low := sink.byType(EventBalanceLow)[0].Data
	require.EqualValues(t, 7, low["user_id"])
	require.EqualValues(t, 5, low["threshold"])

	EmitBalanceChanged(ctx, 8, 1, 2, "add")
	require.Empty(t, sink.byType(EventBalanceChanged), "only watched users")
	EmitBalanceChanged(ctx, 7, 10, 20, "add")
	require.Len(t, sink.byType(EventBalanceChanged), 1)
	require.Len(t, sink.byType(EventBalanceLow), 1, "20 is above the level")
}

func TestUpdateUserBalance_EmitsBalanceChangedAndLowOnce(t *testing.T) {
	sink := &evtSink{watched: map[int64]float64{7: 5}}
	installEvtSink(t, sink)
	repo := &balanceUserRepoStub{userRepoStub: &userRepoStub{user: &User{ID: 7, Balance: 10}}}
	svc := &adminServiceImpl{userRepo: repo, redeemCodeRepo: &balanceRedeemRepoStub{redeemRepoStub: &redeemRepoStub{}}}

	_, err := svc.UpdateUserBalance(context.Background(), 7, 2, "set", "")
	require.NoError(t, err)
	require.Len(t, sink.byType(EventBalanceChanged), 1)
	require.Equal(t, "set", sink.byType(EventBalanceChanged)[0].Data["op"])
	require.Len(t, sink.byType(EventBalanceLow), 1)

	_, err = svc.UpdateUserBalance(context.Background(), 7, 0, "add", "") // no change: nothing
	require.NoError(t, err)
	require.Len(t, sink.byType(EventBalanceChanged), 1)
}

type evtAccountRepo struct{ *accountRepoStub }

func (evtAccountRepo) GetByID(_ context.Context, id int64) (*Account, error) {
	return &Account{ID: id, Name: "seat-twelve", Platform: PlatformOpenAI}, nil
}

type evtGroupRepo struct{ *groupRepoStub }

func (evtGroupRepo) GetByIDLite(context.Context, int64) (*Group, error) {
	return &Group{ID: 5, Name: "g5"}, nil
}

func TestDeleteGroupAndAccount_EmitOnceAfterSuccess(t *testing.T) {
	sink := &evtSink{}
	installEvtSink(t, sink)
	repo := &groupRepoStub{}
	svc := &adminServiceImpl{groupRepo: evtGroupRepo{repo}}
	require.NoError(t, svc.DeleteGroup(context.Background(), 5))
	ev := sink.byType(EventGroupChanged)
	require.Len(t, ev, 1)
	require.Equal(t, "deleted", ev[0].Data["change"])
	require.Equal(t, "g5", ev[0].Data["name"])

	failing := &adminServiceImpl{groupRepo: evtGroupRepo{&groupRepoStub{deleteErr: ErrGroupNotFound}}}
	require.Error(t, failing.DeleteGroup(context.Background(), 5))
	require.Len(t, sink.byType(EventGroupChanged), 1, "a failed delete emits nothing")

	acc := evtAccountRepo{&accountRepoStub{}}
	asvc := &adminServiceImpl{accountRepo: acc}
	require.NoError(t, asvc.DeleteAccount(context.Background(), 12))
	del := sink.byType(EventSeatDeleted)
	require.Len(t, del, 1)
	require.EqualValues(t, 12, del[0].Data["account_id"])
	require.Equal(t, "seat-twelve", del[0].Data["account_name"], "the name is read before the row is gone")
}

func TestEventChangedFields_NamesOnly(t *testing.T) {
	prio := 2
	got := eventChangedFields(&UpdateAccountInput{Name: "x", Credentials: map[string]any{"api_key": "sk-secret"}, Priority: &prio})
	require.Equal(t, []string{"credentials", "name", "priority"}, got)
	raw, _ := json.Marshal(got)
	require.NotContains(t, string(raw), "sk-secret")
	require.Empty(t, eventChangedFields(&UpdateAccountInput{}))
}

func TestSettingsSave_EmitsChangedKeyNamesOnly(t *testing.T) {
	sink := &evtSink{}
	installEvtSink(t, sink)
	repo := newMockSettingRepo()
	repo.data["site_name"] = "old"
	repo.data["smtp_password"] = "hunter2"
	svc := &SettingService{settingRepo: repo}

	require.NoError(t, svc.setMultipleEmitting(context.Background(), map[string]string{
		"site_name": "new", "smtp_password": "hunter2", "brand_new": "v", "events_webhook_config": "{}",
	}))
	ev := sink.byType(EventSettingsChanged)
	require.Len(t, ev, 1)
	require.Equal(t, []string{"brand_new", "site_name"}, ev[0].Data["changed_keys"])
	raw, _ := json.Marshal(ev[0].Data)
	require.NotContains(t, string(raw), "hunter2")

	require.NoError(t, svc.setMultipleEmitting(context.Background(), map[string]string{"site_name": "new"}))
	require.Len(t, sink.byType(EventSettingsChanged), 1, "nothing changed: no event")
}

func TestPricesChanged_OnlyWhenAPriceDiffers(t *testing.T) {
	sink := &evtSink{}
	installEvtSink(t, sink)
	oldT := map[string]*LiteLLMModelPricing{"a": {InputCostPerToken: 1, OutputCostPerToken: 2}, "b": {InputCostPerToken: 3}}
	same := map[string]*LiteLLMModelPricing{"a": {InputCostPerToken: 1, OutputCostPerToken: 2}, "b": {InputCostPerToken: 3}}
	emitPricesChanged("remote", oldT, same, "h1")
	emitPricesChanged("remote", nil, same, "h1") // first load
	require.Empty(t, sink.byType(EventPricesChanged))

	next := map[string]*LiteLLMModelPricing{"a": {InputCostPerToken: 1, OutputCostPerToken: 4}, "b": {InputCostPerToken: 3}, "c": {InputCostPerToken: 9}}
	emitPricesChanged("custom", oldT, next, "h2")
	ev := sink.byType(EventPricesChanged)
	require.Len(t, ev, 1)
	require.Equal(t, "custom", ev[0].Data["source"])
	require.Equal(t, "h2", ev[0].Data["hash"])
	models := ev[0].Data["models"].([]map[string]any)
	names := []string{}
	for _, m := range models {
		names = append(names, m["model"].(string))
	}
	sort.Strings(names)
	require.Equal(t, []string{"a", "c"}, names)
	require.Equal(t, 4.0, models[0]["new_output"])
	require.Equal(t, 2.0, models[0]["old_output"])
}

func TestNoopSinkByDefault(t *testing.T) {
	SetEventSink(nil)
	require.False(t, EventWanted(EventSeatStatus))
	EmitSeatStatus(context.Background(), 1, StatusError, "auth") // must not panic or block
}

func TestEventsService_ModelsCheckWaitsOutTheListCacheAfterAnEdit(t *testing.T) {
	h := newEvtHarness(t, nil, nil, nil)
	h.configure(t, func(c *EventsWebhookConfig) { c.EventTypes = []string{EventModelsChanged} })
	ctx := context.Background()
	h.data.setGroups(EventGroupModels{GroupID: 1, Name: "g", Models: []string{"a"}})
	h.svc.RunOnce(ctx) // the check at start
	require.Len(t, h.outbox.bodies(EventModelsChanged), 1)

	h.data.setGroups(EventGroupModels{GroupID: 1, Name: "g", Models: []string{"a", "b"}})
	h.svc.ScheduleModelsCheck() // an admin edit
	*h.clock = h.clock.Add(5 * time.Second)
	h.svc.RunOnce(ctx)
	require.Len(t, h.outbox.bodies(EventModelsChanged), 1, "too soon: the gateway cache may still hold the old list")
	*h.clock = h.clock.Add(20 * time.Second)
	h.svc.RunOnce(ctx)
	require.Len(t, h.outbox.bodies(EventModelsChanged), 2)
	h.svc.RunOnce(ctx)
	require.Len(t, h.outbox.bodies(EventModelsChanged), 2, "one check per edit")
}

// The lock lives leaderTTL; a hung receiver must not keep a replica sending past
// half of it, or a peer could take the lock and send the same rows again.
func TestEventsService_DeliveryStaysInsideHalfTheLockLifetime(t *testing.T) {
	h := newEvtHarness(t, &evtLockCache{}, nil, nil)
	h.srv.Close()
	slow := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		h.mu.Lock()
		h.hits = append(h.hits, evtHit{ID: r.Header.Get(EventIDHeader)})
		h.mu.Unlock()
		select {
		case <-time.After(3 * time.Second):
		case <-r.Context().Done():
		}
	}))
	t.Cleanup(slow.Close)
	h.svc.httpClient = slow.Client()
	h.svc.leaderTTL = 400 * time.Millisecond
	h.configure(t, func(c *EventsWebhookConfig) { c.URL = slow.URL; c.EventTypes = []string{EventSeatStatus} })
	ctx := context.Background()
	for i := 0; i < 20; i++ {
		h.svc.Emit(ctx, EventSeatStatus, map[string]any{"account_id": int64(i + 1), "status": "error"})
	}

	start := time.Now()
	h.svc.RunOnce(ctx)
	elapsed := time.Since(start)

	require.Less(t, elapsed, 400*time.Millisecond, "a cycle ends before its lock could expire")
	require.Equal(t, 1, h.hitCount(), "a dead receiver stops the batch at the first failure")
	pending := 0
	for _, id := range h.outbox.ord {
		if h.outbox.status(id) == "pending" {
			pending++
		}
	}
	require.Equal(t, 20, pending, "nothing is lost: all still pending")
	require.Equal(t, "timeout", h.outbox.recs[h.outbox.ord[0]].LastError)
}

func TestEventsService_SameEventKeyWritesOneRow(t *testing.T) {
	h := newEvtHarness(t, nil, nil, nil)
	h.configure(t, nil)
	ctx := context.Background()
	d := func() map[string]any {
		return map[string]any{"account_id": int64(4), "kind": "rate_limit", "until": "2026-11-20T13:00:00Z", eventKeyField: "4|rate_limit|2026-11-20T13:00:00Z"}
	}
	h.svc.Emit(ctx, EventSeatUsage, d())
	h.svc.Emit(ctx, EventSeatUsage, d()) // what a second replica would write
	require.Len(t, h.outbox.bodies(EventSeatUsage), 1)
	body := h.outbox.bodies(EventSeatUsage)[0]
	require.NotContains(t, body["data"], eventKeyField, "the key is internal")
	other := d()
	other[eventKeyField] = "4|rate_limit|2026-11-20T14:00:00Z"
	h.svc.Emit(ctx, EventSeatUsage, other)
	require.Len(t, h.outbox.bodies(EventSeatUsage), 2)
}

// Request paths call Wants/BalanceWatch constantly: after the first load they
// must never wait for the database.
func TestEventsService_ConfigReadsNeverBlockOnTheDatabase(t *testing.T) {
	h := newEvtHarness(t, nil, nil, nil)
	h.configure(t, func(c *EventsWebhookConfig) { c.BalanceUserIDs = []int64{7}; c.BalanceThreshold = 3 })
	require.True(t, h.svc.Wants(EventSeatStatus)) // first load

	h.repo.mu.Lock() // a stuck database: the setting repository now blocks
	done := make(chan struct{})
	go func() {
		*h.clock = h.clock.Add(time.Hour) // the snapshot is stale: a refresh is due
		for i := 0; i < 100; i++ {
			h.svc.Wants(EventSeatStatus)
			_, _ = h.svc.BalanceWatch(7)
		}
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("config reads waited for the database")
	}
	h.repo.mu.Unlock()
	th, ok := h.svc.BalanceWatch(7)
	require.True(t, ok)
	require.EqualValues(t, 3, th)
}
