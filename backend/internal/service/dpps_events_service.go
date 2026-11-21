package service

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/pkg/logger"
	"github.com/google/uuid"
)

const (
	dppsSettingKeyEventsWebhookConfig = "dpps_events_webhook_config"
	dppsSettingKeyEventsWebhookState  = "dpps_events_webhook_state"

	dppsEventsLeaderLockKey = "dpps:events:webhook:sender:leader"
	dppsEventsLeaderTTL     = 60 * time.Second

	dppsEventsRetryBase     = 10 * time.Second
	dppsEventsRetryMax      = time.Hour
	dppsEventsRetryDeadline = 24 * time.Hour
	dppsEventsSendTimeout   = 10 * time.Second
	dppsEventsTickInterval  = 5 * time.Second
	dppsEventsConfigTTL     = 15 * time.Second
	dppsEventsBatchSize     = 50
	dppsEventsPruneAfter    = 7 * 24 * time.Hour
	dppsEventsLimitedMerge  = time.Minute
	dppsEventsModelsRecheck = 30 * time.Minute
	// dppsEventsModelsSettle waits out the gateway's 15 s model-list cache so a check
	// after an edit never reads the list from before it.
	dppsEventsModelsSettle = 20 * time.Second
	dppsEventsMaxBodyBytes = 512 * 1024

	// DppsEventSignatureHeader and DppsEventIDHeader are the two headers every POST carries.
	DppsEventSignatureHeader = "X-Event-Signature"
	DppsEventIDHeader        = "X-Event-Id"
)

var (
	ErrDppsEventsWebhookURLInvalid = errors.New("events webhook url must be https and must not point at a private address")
	ErrDppsEventsSnapshotTime      = errors.New("snapshot_time_utc must be HH:MM (UTC) or empty")
	ErrDppsEventsUnknownType       = errors.New("unknown event type")
)

// DppsEventsWebhookConfig is the admin-editable configuration. Off by default.
// Secret is encrypted at rest and never returned by Get.
type DppsEventsWebhookConfig struct {
	Enabled          bool     `json:"enabled"`
	URL              string   `json:"url"`
	Secret           string   `json:"secret,omitempty"`
	EventTypes       []string `json:"event_types"`       // empty = all types
	SnapshotTimeUTC  string   `json:"snapshot_time_utc"` // "HH:MM", empty = no daily snapshot
	BalanceThreshold float64  `json:"balance_threshold"` // balance.low fires when a watched balance crosses below
	BalanceUserIDs   []int64  `json:"balance_user_ids"`  // users watched for balance.low / balance.changed
}

// DppsEventOutboxRecord is one row of the dpps_event_outbox table.
type DppsEventOutboxRecord struct {
	ID            string
	Type          string
	Body          []byte
	CreatedAt     time.Time
	NextAttemptAt time.Time
	Attempts      int
}

// DppsEventOutboxRepository stores events until they are delivered.
type DppsEventOutboxRepository interface {
	Insert(ctx context.Context, rec DppsEventOutboxRecord) error
	ListDue(ctx context.Context, now time.Time, limit int) ([]DppsEventOutboxRecord, error)
	MarkSent(ctx context.Context, id string, now time.Time) error
	Reschedule(ctx context.Context, id string, attempts int, next time.Time, errClass string) error
	MarkFailed(ctx context.Context, id string, attempts int, errClass string, now time.Time) error
	Prune(ctx context.Context, before time.Time) (int64, error)
}

// DppsEventGroupModels is one group's served model list.
type DppsEventGroupModels struct {
	GroupID int64
	Name    string
	Models  []string
}

// DppsEventsDataSource reads the state the worker needs (names, model lists, balances).
type DppsEventsDataSource interface {
	AccountBrief(ctx context.Context, id int64) (map[string]any, bool)
	GroupModelLists(ctx context.Context) ([]DppsEventGroupModels, error)
	SeatSnapshots(ctx context.Context) ([]map[string]any, error)
	UserBalance(ctx context.Context, userID int64) (float64, bool)
	PriceTableHash() string
}

type dppsEventsGroupState struct {
	Hash   string   `json:"hash"`
	Models []string `json:"models"`
}

type dppsEventsState struct {
	Groups          map[string]dppsEventsGroupState `json:"groups"`
	LastVersion     string                          `json:"last_version"`
	LastSnapshotDay string                          `json:"last_snapshot_day"`
}

// DppsEventsService is the outgoing events webhook: an DppsEventSink that writes the
// outbox, plus the leader-locked delivery worker.
type dppsEventsCfgSnapshot struct {
	cfg *DppsEventsWebhookConfig
	at  time.Time
}

type DppsEventsService struct {
	settingRepo SettingRepository
	outbox      DppsEventOutboxRepository
	encryptor   SecretEncryptor
	keyReady    func() bool
	data        DppsEventsDataSource
	lockCache   LeaderLockCache
	db          *sql.DB
	version     string
	commit      string
	instanceID  string
	leaderTTL   time.Duration // lock lifetime; all work under it must end well inside it

	httpClient    *http.Client
	allowInsecure bool // tests only: skip the https/private-address checks
	now           func() time.Time

	cfgPtr        atomic.Pointer[dppsEventsCfgSnapshot]
	cfgRefreshing atomic.Bool
	cfgLoadMu     sync.Mutex // only the very first load blocks; readers never wait on the database

	mergeMu     sync.Mutex
	limitedSeen map[string]time.Time

	modelsDirtyAt atomic.Int64 // unix nanos of the newest edit not yet checked (0 = none)
	startOnce     sync.Once
	stopOnce      sync.Once
	stopCh        chan struct{}
	wg            sync.WaitGroup
	startedSent   bool
	lastModelsRun time.Time
}

func NewDppsEventsService(settingRepo SettingRepository, outbox DppsEventOutboxRepository, encryptor SecretEncryptor, keyReady func() bool,
	data DppsEventsDataSource, lockCache LeaderLockCache, db *sql.DB, version, commit string) *DppsEventsService {
	s := &DppsEventsService{
		settingRepo: settingRepo, outbox: outbox, encryptor: encryptor, keyReady: keyReady,
		data: data, lockCache: lockCache, db: db, version: version, commit: commit,
		instanceID:  uuid.NewString(),
		leaderTTL:   dppsEventsLeaderTTL,
		now:         time.Now,
		limitedSeen: map[string]time.Time{},
		stopCh:      make(chan struct{}),
	}
	s.httpClient = &http.Client{
		Timeout:   dppsEventsSendTimeout,
		Transport: &http.Transport{DialContext: safeDialContext, TLSHandshakeTimeout: 5 * time.Second, DisableKeepAlives: true},
		// A receiver must answer directly: a redirect could point somewhere unvetted.
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
	}
	return s
}

// ---- signature -------------------------------------------------------------

// DppsSignEventBody returns "t=<unix>,v1=<hex HMAC-SHA256 over "t.body">".
func DppsSignEventBody(secret string, ts time.Time, body []byte) string {
	t := strconv.FormatInt(ts.Unix(), 10)
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write([]byte(t))
	mac.Write([]byte("."))
	mac.Write(body)
	return "t=" + t + ",v1=" + hex.EncodeToString(mac.Sum(nil))
}

// DppsVerifyEventSignature checks a header against a body and secret and rejects a
// timestamp further than tolerance from now. It is what a receiver does.
func DppsVerifyEventSignature(secret, header string, body []byte, now time.Time, tolerance time.Duration) bool {
	var t, v1 string
	for _, part := range strings.Split(header, ",") {
		k, v, ok := strings.Cut(strings.TrimSpace(part), "=")
		if !ok {
			continue
		}
		switch k {
		case "t":
			t = v
		case "v1":
			v1 = v
		}
	}
	sec, err := strconv.ParseInt(t, 10, 64)
	if err != nil || v1 == "" {
		return false
	}
	if d := now.Sub(time.Unix(sec, 0)); d > tolerance || d < -tolerance {
		return false
	}
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write([]byte(t))
	mac.Write([]byte("."))
	mac.Write(body)
	got, err := hex.DecodeString(v1)
	if err != nil {
		return false
	}
	return hmac.Equal(got, mac.Sum(nil))
}

// ---- configuration ---------------------------------------------------------

func dppsValidateEventsWebhookURL(raw string) error {
	u, err := url.Parse(strings.TrimSpace(raw))
	if err != nil || u.Scheme != "https" || u.Host == "" || u.User != nil || u.Fragment != "" {
		return ErrDppsEventsWebhookURLInvalid
	}
	ctx, cancel := context.WithTimeout(context.Background(), monitorEndpointResolveTimeout)
	defer cancel()
	blocked, err := isPrivateOrLoopbackHost(ctx, u.Hostname())
	if err != nil || blocked {
		return ErrDppsEventsWebhookURLInvalid
	}
	return nil
}

func dppsNormalizeEventsConfig(in *DppsEventsWebhookConfig) error {
	in.URL = strings.TrimSpace(in.URL)
	in.SnapshotTimeUTC = strings.TrimSpace(in.SnapshotTimeUTC)
	if in.SnapshotTimeUTC != "" {
		if _, err := time.Parse("15:04", in.SnapshotTimeUTC); err != nil {
			return ErrDppsEventsSnapshotTime
		}
	}
	seen := map[string]bool{}
	types := []string{}
	for _, t := range in.EventTypes {
		t = strings.TrimSpace(t)
		if t == "" || seen[t] {
			continue
		}
		if !dppsIsKnownEventType(t) {
			return fmt.Errorf("%w: %s", ErrDppsEventsUnknownType, t)
		}
		seen[t] = true
		types = append(types, t)
	}
	sort.Strings(types)
	in.EventTypes = types
	if in.BalanceThreshold < 0 {
		in.BalanceThreshold = 0
	}
	if in.BalanceUserIDs == nil {
		in.BalanceUserIDs = []int64{}
	}
	return nil
}

func (s *DppsEventsService) loadConfigRaw(ctx context.Context) (*DppsEventsWebhookConfig, error) {
	raw, err := s.settingRepo.GetValue(ctx, dppsSettingKeyEventsWebhookConfig)
	if err != nil {
		if errors.Is(err, ErrSettingNotFound) {
			return &DppsEventsWebhookConfig{}, nil
		}
		return nil, err
	}
	var cfg DppsEventsWebhookConfig
	if strings.TrimSpace(raw) == "" {
		return &cfg, nil
	}
	if err := json.Unmarshal([]byte(raw), &cfg); err != nil {
		return nil, err
	}
	return &cfg, nil
}

// loadConfigDecrypted reads the stored configuration and decrypts the secret.
func (s *DppsEventsService) loadConfigDecrypted() (*DppsEventsWebhookConfig, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	cfg, err := s.loadConfigRaw(ctx)
	if err != nil {
		return nil, err
	}
	if cfg.Secret != "" && s.encryptor != nil {
		if plain, derr := s.encryptor.Decrypt(cfg.Secret); derr == nil {
			cfg.Secret = plain
		} else {
			cfg.Secret = ""
		}
	}
	return cfg, nil
}

// refreshConfig reloads the snapshot. On a database error the old one stays.
func (s *DppsEventsService) refreshConfig() {
	cfg, err := s.loadConfigDecrypted()
	if err != nil {
		if s.cfgPtr.Load() == nil {
			s.cfgPtr.Store(&dppsEventsCfgSnapshot{cfg: &DppsEventsWebhookConfig{}, at: s.now()})
		}
		return
	}
	s.cfgPtr.Store(&dppsEventsCfgSnapshot{cfg: cfg, at: s.now()})
}

// config returns the cached configuration with the secret decrypted. It runs on
// request paths (Wants, BalanceWatch), so after the first load it never waits
// for the database: a stale snapshot is served while one goroutine refreshes it.
func (s *DppsEventsService) config() *DppsEventsWebhookConfig {
	snap := s.cfgPtr.Load()
	if snap == nil {
		s.cfgLoadMu.Lock()
		if snap = s.cfgPtr.Load(); snap == nil {
			s.refreshConfig()
			snap = s.cfgPtr.Load()
		}
		s.cfgLoadMu.Unlock()
		return snap.cfg
	}
	if s.now().Sub(snap.at) >= dppsEventsConfigTTL && s.cfgRefreshing.CompareAndSwap(false, true) {
		go func() {
			defer s.cfgRefreshing.Store(false)
			s.refreshConfig()
		}()
	}
	return snap.cfg
}

func (s *DppsEventsService) invalidateConfig() {
	s.cfgPtr.Store(nil)
}

// Get returns the stored configuration without the secret, plus whether one is set.
func (s *DppsEventsService) Get(ctx context.Context) (*DppsEventsWebhookConfig, bool, error) {
	cfg, err := s.loadConfigRaw(ctx)
	if err != nil {
		return nil, false, err
	}
	configured := cfg.Secret != ""
	cfg.Secret = ""
	if cfg.EventTypes == nil {
		cfg.EventTypes = []string{}
	}
	if cfg.BalanceUserIDs == nil {
		cfg.BalanceUserIDs = []int64{}
	}
	return cfg, configured, nil
}

// Update saves the configuration. An empty Secret keeps the stored one.
func (s *DppsEventsService) Update(ctx context.Context, in DppsEventsWebhookConfig) (*DppsEventsWebhookConfig, error) {
	if err := dppsNormalizeEventsConfig(&in); err != nil {
		return nil, err
	}
	old, err := s.loadConfigRaw(ctx)
	if err != nil {
		return nil, err
	}
	if in.URL != "" && !s.allowInsecure {
		if err := dppsValidateEventsWebhookURL(in.URL); err != nil {
			return nil, err
		}
	}
	if in.Secret == "" {
		in.Secret = old.Secret
	} else {
		if s.keyReady != nil && !s.keyReady() {
			return nil, ErrSecretEncryptionKeyNotConfigured
		}
		enc, err := s.encryptor.Encrypt(in.Secret)
		if err != nil {
			return nil, fmt.Errorf("encrypt secret: %w", err)
		}
		in.Secret = enc
	}
	if in.Enabled && (in.URL == "" || in.Secret == "") {
		return nil, errors.New("enabled requires a url and a secret")
	}
	data, err := json.Marshal(in)
	if err != nil {
		return nil, err
	}
	if err := s.settingRepo.Set(ctx, dppsSettingKeyEventsWebhookConfig, string(data)); err != nil {
		return nil, err
	}
	s.invalidateConfig()
	in.Secret = ""
	return &in, nil
}

// ---- DppsEventSink -------------------------------------------------------------

func (cfg *DppsEventsWebhookConfig) wants(t string) bool {
	if !cfg.Enabled || cfg.URL == "" || cfg.Secret == "" {
		return false
	}
	if len(cfg.EventTypes) == 0 {
		return true
	}
	for _, k := range cfg.EventTypes {
		if k == t {
			return true
		}
	}
	return false
}

func (s *DppsEventsService) Wants(t string) bool { return s.config().wants(t) }

// Emit writes one event to the outbox. It never returns an error: a failure is
// logged and the daily snapshot covers the gap.
func (s *DppsEventsService) Emit(ctx context.Context, eventType string, data map[string]any) {
	if !s.Wants(eventType) {
		return
	}
	if ctx == nil || ctx.Err() != nil {
		ctx = context.Background()
	}
	if eventType == DppsEventSeatLimited && s.mergeLimited(data) {
		return
	}
	if id, ok := data["account_id"].(int64); ok && s.data != nil {
		if brief, found := s.data.AccountBrief(ctx, id); found {
			merged := make(map[string]any, len(data)+len(brief))
			for k, v := range brief {
				merged[k] = v
			}
			for k, v := range data {
				merged[k] = v
			}
			data = merged
		}
	}
	if err := s.enqueue(ctx, eventType, data); err != nil {
		logger.LegacyPrintf("service.events", "[Events] enqueue %s failed: %v", eventType, err)
	}
}

// mergeLimited reports true when the same seat was already announced limited
// (same kind) within the merge window.
func (s *DppsEventsService) mergeLimited(data map[string]any) bool {
	key := fmt.Sprintf("%v|%v", data["account_id"], data["kind"])
	now := s.now()
	s.mergeMu.Lock()
	defer s.mergeMu.Unlock()
	if last, ok := s.limitedSeen[key]; ok && now.Sub(last) < dppsEventsLimitedMerge {
		return true
	}
	s.limitedSeen[key] = now
	if len(s.limitedSeen) > 4096 {
		for k, t := range s.limitedSeen {
			if now.Sub(t) > dppsEventsLimitedMerge {
				delete(s.limitedSeen, k)
			}
		}
	}
	return false
}

// dppsEventKeyField is an internal key an emit point may set: events with the same
// key get the same id, so two replicas reporting the same crossing write one
// outbox row (the insert ignores a repeated id).
const dppsEventKeyField = "_event_key"

var dppsEventIDNamespace = uuid.MustParse("6f1a2d7e-3b54-4c1e-9a8d-5e2b7c0d41aa")

func (s *DppsEventsService) enqueue(ctx context.Context, eventType string, data map[string]any) error {
	now := s.now().UTC()
	id := uuid.NewString()
	if key, ok := data[dppsEventKeyField].(string); ok && key != "" {
		id = uuid.NewSHA1(dppsEventIDNamespace, []byte(eventType+"|"+key)).String()
		clean := make(map[string]any, len(data))
		for k, v := range data {
			if k != dppsEventKeyField {
				clean[k] = v
			}
		}
		data = clean
	}
	env := map[string]any{
		"id":              id,
		"type":            eventType,
		"at":              now.Format(time.RFC3339),
		"sub2api_version": s.version,
		"data":            dppsSanitizeEventData(data),
	}
	body, err := json.Marshal(env)
	if err != nil {
		return err
	}
	if len(body) > dppsEventsMaxBodyBytes {
		env["data"] = map[string]any{"truncated": true}
		if body, err = json.Marshal(env); err != nil {
			return err
		}
	}
	return s.outbox.Insert(ctx, DppsEventOutboxRecord{
		ID: env["id"].(string), Type: eventType, Body: body, CreatedAt: now, NextAttemptAt: now,
	})
}

// BalanceWatch reports the threshold when userID is a watched user.
func (s *DppsEventsService) BalanceWatch(userID int64) (float64, bool) {
	cfg := s.config()
	for _, id := range cfg.BalanceUserIDs {
		if id == userID {
			return cfg.BalanceThreshold, true
		}
	}
	return 0, false
}

// ScheduleModelsCheck asks the worker to re-hash every group's served list once
// the gateway's list cache has had time to expire.
func (s *DppsEventsService) ScheduleModelsCheck() {
	s.modelsDirtyAt.Store(s.now().UnixNano())
}

// ---- optional sink capabilities, reached through helpers ---------------------

type dppsEventsBalanceWatcher interface {
	BalanceWatch(userID int64) (float64, bool)
}

type dppsEventsModelsScheduler interface{ ScheduleModelsCheck() }

// DppsEventsModelsMayHaveChanged is called after account, group or channel edits.
func DppsEventsModelsMayHaveChanged() {
	if m, ok := dppsCurrentEventSink().(dppsEventsModelsScheduler); ok {
		m.ScheduleModelsCheck()
	}
}

// DppsEmitBalanceLowIfCrossed is called after a committed balance change. It emits
// balance.low when a watched user's balance crosses below the threshold.
func DppsEmitBalanceLowIfCrossed(ctx context.Context, userID int64, oldBalance, newBalance float64) {
	w, ok := dppsCurrentEventSink().(dppsEventsBalanceWatcher)
	if !ok {
		return
	}
	threshold, watched := w.BalanceWatch(userID)
	if !watched || threshold <= 0 || !(oldBalance >= threshold && newBalance < threshold) {
		return
	}
	DppsEmitEvent(ctx, DppsEventBalanceLow, map[string]any{"user_id": userID, "balance": newBalance, "threshold": threshold})
}

// ---- worker ----------------------------------------------------------------

func (s *DppsEventsService) Start() {
	if s == nil {
		return
	}
	s.startOnce.Do(func() {
		s.wg.Add(1)
		go s.run()
	})
}

func (s *DppsEventsService) Stop() {
	if s == nil {
		return
	}
	s.stopOnce.Do(func() {
		if s.stopCh != nil {
			close(s.stopCh)
		}
	})
	s.wg.Wait()
}

func (s *DppsEventsService) run() {
	defer s.wg.Done()
	s.modelsDirtyAt.Store(s.now().Add(-dppsEventsModelsSettle).UnixNano()) // "and at start": due at once
	t := time.NewTicker(dppsEventsTickInterval)
	defer t.Stop()
	for {
		select {
		case <-s.stopCh:
			return
		case <-t.C:
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
			s.RunOnce(ctx)
			cancel()
		}
	}
}

// RunOnce is one worker cycle: take the leader lock, run the checks, deliver.
// Everything done under the lock has a budget of half its lifetime, so the lock
// can never expire while this replica is still sending (two senders at once).
func (s *DppsEventsService) RunOnce(ctx context.Context) {
	s.refreshConfig() // the worker is off the request path: always read fresh
	cfg := s.config()
	if !cfg.Enabled || cfg.URL == "" || cfg.Secret == "" {
		return
	}
	release, ok := tryAcquireSingletonLeaderLock(ctx, s.lockCache, s.db, dppsEventsLeaderLockKey, s.instanceID, s.leaderTTL)
	if !ok {
		return
	}
	defer release()
	work, cancel := context.WithTimeout(ctx, s.leaderTTL/2)
	defer cancel()

	s.checkVersion(work)
	s.maybeCheckModels(work)
	s.maybeSnapshot(work, cfg)
	s.deliverDue(work, cfg)
	if _, err := s.outbox.Prune(work, s.now().Add(-dppsEventsPruneAfter)); err != nil {
		logger.LegacyPrintf("service.events", "[Events] prune failed: %v", err)
	}
}

func (s *DppsEventsService) loadState(ctx context.Context) dppsEventsState {
	st := dppsEventsState{Groups: map[string]dppsEventsGroupState{}}
	if raw, err := s.settingRepo.GetValue(ctx, dppsSettingKeyEventsWebhookState); err == nil && raw != "" {
		_ = json.Unmarshal([]byte(raw), &st)
	}
	if st.Groups == nil {
		st.Groups = map[string]dppsEventsGroupState{}
	}
	return st
}

func (s *DppsEventsService) saveState(ctx context.Context, st dppsEventsState) {
	if raw, err := json.Marshal(st); err == nil {
		if err := s.settingRepo.Set(ctx, dppsSettingKeyEventsWebhookState, string(raw)); err != nil {
			logger.LegacyPrintf("service.events", "[Events] save state failed: %v", err)
		}
	}
}

// checkVersion emits system.started once per process and system.version_changed
// when the version differs from the last one sent.
func (s *DppsEventsService) checkVersion(ctx context.Context) {
	if s.startedSent {
		return
	}
	s.startedSent = true
	data := map[string]any{"version": s.version}
	if s.commit != "" {
		data["commit"] = s.commit
	}
	s.Emit(ctx, DppsEventSystemStarted, data)
	st := s.loadState(ctx)
	if st.LastVersion != "" && st.LastVersion != s.version {
		d := map[string]any{"version": s.version, "previous_version": st.LastVersion}
		if s.commit != "" {
			d["commit"] = s.commit
		}
		s.Emit(ctx, DppsEventVersionChanged, d)
	}
	if st.LastVersion != s.version {
		st.LastVersion = s.version
		s.saveState(ctx, st)
	}
}

func dppsHashModelList(models []string) string {
	h := sha256.Sum256([]byte(strings.Join(models, "\n")))
	return hex.EncodeToString(h[:])
}

func dppsSortedUnique(in []string) []string {
	set := map[string]struct{}{}
	for _, m := range in {
		if m != "" {
			set[m] = struct{}{}
		}
	}
	out := make([]string, 0, len(set))
	for m := range set {
		out = append(out, m)
	}
	sort.Strings(out)
	return out
}

func dppsDiffSorted(old, cur []string) (added, removed []string) {
	o := map[string]struct{}{}
	for _, m := range old {
		o[m] = struct{}{}
	}
	c := map[string]struct{}{}
	for _, m := range cur {
		c[m] = struct{}{}
		if _, ok := o[m]; !ok {
			added = append(added, m)
		}
	}
	for _, m := range old {
		if _, ok := c[m]; !ok {
			removed = append(removed, m)
		}
	}
	return added, removed
}

func (s *DppsEventsService) maybeCheckModels(ctx context.Context) {
	dirtyAt := s.modelsDirtyAt.Load()
	dirty := dirtyAt != 0 && s.now().Sub(time.Unix(0, dirtyAt)) >= dppsEventsModelsSettle
	if !dirty && !s.lastModelsRun.IsZero() && s.now().Sub(s.lastModelsRun) < dppsEventsModelsRecheck {
		return
	}
	if dirty {
		s.modelsDirtyAt.CompareAndSwap(dirtyAt, 0)
	}
	s.lastModelsRun = s.now()
	s.CheckModels(ctx)
}

// CheckModels emits models.changed for every group whose served list hash
// differs from the last one sent. A group seen for the first time reports its
// whole list as added.
func (s *DppsEventsService) CheckModels(ctx context.Context) {
	if s.data == nil || !s.Wants(DppsEventModelsChanged) {
		return
	}
	groups, err := s.data.GroupModelLists(ctx)
	if err != nil {
		logger.LegacyPrintf("service.events", "[Events] models check failed: %v", err)
		return
	}
	st := s.loadState(ctx)
	changed := false
	seen := map[string]bool{}
	for _, g := range groups {
		key := strconv.FormatInt(g.GroupID, 10)
		seen[key] = true
		models := dppsSortedUnique(g.Models)
		h := dppsHashModelList(models)
		prev, had := st.Groups[key]
		if had && prev.Hash == h {
			continue
		}
		added, removed := dppsDiffSorted(prev.Models, models)
		s.Emit(ctx, DppsEventModelsChanged, map[string]any{
			"group_id": g.GroupID, "group_name": g.Name,
			"added": dppsNonNil(added), "removed": dppsNonNil(removed), "models": models,
		})
		st.Groups[key] = dppsEventsGroupState{Hash: h, Models: models}
		changed = true
	}
	for key := range st.Groups {
		if !seen[key] {
			delete(st.Groups, key) // group gone; its removal is a group.changed event
			changed = true
		}
	}
	if changed {
		s.saveState(ctx, st)
	}
}

func dppsNonNil(in []string) []string {
	if in == nil {
		return []string{}
	}
	return in
}

func (s *DppsEventsService) maybeSnapshot(ctx context.Context, cfg *DppsEventsWebhookConfig) {
	if cfg.SnapshotTimeUTC == "" || !cfg.wants(DppsEventSnapshotDaily) || s.data == nil {
		return
	}
	hm, err := time.Parse("15:04", cfg.SnapshotTimeUTC)
	if err != nil {
		return
	}
	now := s.now().UTC()
	due := time.Date(now.Year(), now.Month(), now.Day(), hm.Hour(), hm.Minute(), 0, 0, time.UTC)
	day := now.Format("2006-01-02")
	if now.Before(due) {
		return
	}
	st := s.loadState(ctx)
	if st.LastSnapshotDay == day {
		return
	}
	s.EmitSnapshot(ctx, cfg)
	st.LastSnapshotDay = day
	s.saveState(ctx, st)
}

// EmitSnapshot writes one snapshot.daily event: every seat, every group's list,
// the version, the price-table hash and the watched balances.
func (s *DppsEventsService) EmitSnapshot(ctx context.Context, cfg *DppsEventsWebhookConfig) {
	data := map[string]any{"version": s.version}
	if seats, err := s.data.SeatSnapshots(ctx); err == nil {
		data["seats"] = seats
	}
	if groups, err := s.data.GroupModelLists(ctx); err == nil {
		list := make([]map[string]any, 0, len(groups))
		for _, g := range groups {
			list = append(list, map[string]any{"group_id": g.GroupID, "group_name": g.Name, "models": dppsSortedUnique(g.Models)})
		}
		data["groups"] = list
	}
	data["price_table_hash"] = s.data.PriceTableHash()
	balances := []map[string]any{}
	for _, uid := range cfg.BalanceUserIDs {
		if b, ok := s.data.UserBalance(ctx, uid); ok {
			balances = append(balances, map[string]any{"user_id": uid, "balance": b})
		}
	}
	data["balances"] = balances
	s.Emit(ctx, DppsEventSnapshotDaily, data)
}

// ---- delivery --------------------------------------------------------------

func dppsEventBackoff(attempts int) time.Duration {
	d := dppsEventsRetryBase
	for i := 1; i < attempts && d < dppsEventsRetryMax; i++ {
		d *= 2
	}
	if d > dppsEventsRetryMax {
		d = dppsEventsRetryMax
	}
	return d
}

func (s *DppsEventsService) deliverDue(ctx context.Context, cfg *DppsEventsWebhookConfig) {
	now := s.now()
	due, err := s.outbox.ListDue(ctx, now, dppsEventsBatchSize)
	if err != nil {
		logger.LegacyPrintf("service.events", "[Events] list due failed: %v", err)
		return
	}
	for _, rec := range due {
		if ctx.Err() != nil {
			return
		}
		if now.Sub(rec.CreatedAt) > dppsEventsRetryDeadline {
			_ = s.outbox.MarkFailed(ctx, rec.ID, rec.Attempts, "deadline", now)
			continue
		}
		errClass := s.send(ctx, cfg, rec)
		attempts := rec.Attempts + 1
		if errClass == "" {
			_ = s.outbox.MarkSent(context.WithoutCancel(ctx), rec.ID, s.now())
			continue
		}
		bg := context.WithoutCancel(ctx)
		next := s.now().Add(dppsEventBackoff(attempts))
		if next.Sub(rec.CreatedAt) > dppsEventsRetryDeadline {
			_ = s.outbox.MarkFailed(bg, rec.ID, attempts, errClass, s.now())
		} else {
			_ = s.outbox.Reschedule(bg, rec.ID, attempts, next, errClass)
		}
		if errClass == "network" || errClass == "timeout" {
			// A dead receiver fails every event in the batch; try again next cycle.
			return
		}
	}
}

// send POSTs one event and returns an error class ("" on success). Only a class
// is kept, never the receiver's text.
func (s *DppsEventsService) send(ctx context.Context, cfg *DppsEventsWebhookConfig, rec DppsEventOutboxRecord) string {
	if !s.allowInsecure {
		if err := dppsValidateEventsWebhookURL(cfg.URL); err != nil {
			return "url_blocked"
		}
	}
	rctx, cancel := context.WithTimeout(ctx, dppsEventsSendTimeout)
	defer cancel()
	req, err := http.NewRequestWithContext(rctx, http.MethodPost, cfg.URL, bytes.NewReader(rec.Body))
	if err != nil {
		return "bad_request"
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set(DppsEventIDHeader, rec.ID)
	req.Header.Set(DppsEventSignatureHeader, DppsSignEventBody(cfg.Secret, s.now(), rec.Body))
	resp, err := s.httpClient.Do(req)
	if err != nil {
		var ne net.Error
		if errors.As(err, &ne) && ne.Timeout() {
			return "timeout"
		}
		return "network"
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		return ""
	}
	return "http_" + strconv.Itoa(resp.StatusCode)
}
