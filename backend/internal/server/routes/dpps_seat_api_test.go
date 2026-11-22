package routes

import (
	"bytes"
	"context"
	cryptorand "crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"log"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"entgo.io/ent/dialect"
	entsql "entgo.io/ent/dialect/sql"
	sqlmock "github.com/DATA-DOG/go-sqlmock"
	dbent "github.com/Wei-Shaw/sub2api/ent"
	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/Wei-Shaw/sub2api/internal/handler"
	adminhandler "github.com/Wei-Shaw/sub2api/internal/handler/admin"
	"github.com/Wei-Shaw/sub2api/internal/pkg/adminaccess"
	"github.com/Wei-Shaw/sub2api/internal/pkg/logger"
	"github.com/Wei-Shaw/sub2api/internal/repository"
	"github.com/Wei-Shaw/sub2api/internal/server/middleware"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/alicebob/miniredis/v2"
	"github.com/gin-gonic/gin"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
)

// ---- in-memory fakes matching real Sub2API contracts -------------------------

type dppsAPISettingRepo struct {
	mu      sync.RWMutex
	data    map[string]string
	getErr  error
	getHook func(key string)
}

func newDppsAPISettingRepo() *dppsAPISettingRepo {
	return &dppsAPISettingRepo{data: make(map[string]string)}
}

func (r *dppsAPISettingRepo) Get(_ context.Context, key string) (*service.Setting, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if r.getErr != nil {
		return nil, r.getErr
	}
	v, ok := r.data[key]
	if !ok {
		return nil, service.ErrSettingNotFound
	}
	return &service.Setting{Key: key, Value: v}, nil
}

func (r *dppsAPISettingRepo) GetValue(_ context.Context, key string) (string, error) {
	if r.getHook != nil {
		r.getHook(key)
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	if r.getErr != nil {
		return "", r.getErr
	}
	v, ok := r.data[key]
	if !ok {
		return "", service.ErrSettingNotFound
	}
	return v, nil
}

func (r *dppsAPISettingRepo) Set(_ context.Context, key, value string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.data[key] = value
	return nil
}

func (r *dppsAPISettingRepo) GetMultiple(_ context.Context, keys []string) (map[string]string, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make(map[string]string, len(keys))
	for _, k := range keys {
		if v, ok := r.data[k]; ok {
			out[k] = v
		}
	}
	return out, nil
}

func (r *dppsAPISettingRepo) SetMultiple(_ context.Context, settings map[string]string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	for k, v := range settings {
		r.data[k] = v
	}
	return nil
}

func (r *dppsAPISettingRepo) GetAll(_ context.Context) (map[string]string, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make(map[string]string, len(r.data))
	for k, v := range r.data {
		out[k] = v
	}
	return out, nil
}

func (r *dppsAPISettingRepo) Delete(_ context.Context, key string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.data[key]; !ok {
		return service.ErrSettingNotFound
	}
	delete(r.data, key)
	return nil
}

type dppsAPIScopedAdminKeyRepo struct {
	service.ScopedAdminKeyRepository
	mu    sync.RWMutex
	items map[string]*service.ScopedAdminKey
}

func newDppsAPIScopedAdminKeyRepo() *dppsAPIScopedAdminKeyRepo {
	return &dppsAPIScopedAdminKeyRepo{items: make(map[string]*service.ScopedAdminKey)}
}

func (r *dppsAPIScopedAdminKeyRepo) InsertScopedAdminKey(_ context.Context, item *service.ScopedAdminKey) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	cp := *item
	r.items[item.ID] = &cp
	return nil
}

func (r *dppsAPIScopedAdminKeyRepo) GetScopedAdminKey(_ context.Context, id string) (*service.ScopedAdminKey, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	item, ok := r.items[id]
	if !ok {
		return nil, service.ErrSettingNotFound
	}
	cp := *item
	return &cp, nil
}

func (r *dppsAPIScopedAdminKeyRepo) TouchScopedAdminKey(context.Context, string, string, time.Time, string, time.Time) error {
	return nil
}

type dppsAPIAuditSpy struct {
	service.AuditLogRepository
	mu     sync.Mutex
	rows   []*service.AuditLog
	onCall func(entry *service.AuditLog)
}

func (a *dppsAPIAuditSpy) Insert(_ context.Context, entry *service.AuditLog) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.onCall != nil {
		a.onCall(entry)
	}
	cp := *entry
	if entry.Extra != nil {
		cp.Extra = make(map[string]any, len(entry.Extra))
		for k, v := range entry.Extra {
			cp.Extra[k] = v
		}
	}
	a.rows = append(a.rows, &cp)
	return nil
}

func (a *dppsAPIAuditSpy) snapshot() []*service.AuditLog {
	a.mu.Lock()
	defer a.mu.Unlock()
	out := make([]*service.AuditLog, len(a.rows))
	copy(out, a.rows)
	return out
}

type dppsAPIUserRepo struct {
	service.UserRepository
	mu              sync.Mutex
	users           map[int64]*service.User
	allowedAdds     []string
	unexpectedCalls int
}

func newDppsAPIUserRepo() *dppsAPIUserRepo {
	return &dppsAPIUserRepo{users: make(map[int64]*service.User)}
}

func (r *dppsAPIUserRepo) GetByID(_ context.Context, id int64) (*service.User, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	u, ok := r.users[id]
	if !ok {
		return nil, service.ErrUserNotFound
	}
	cp := *u
	return &cp, nil
}

func (r *dppsAPIUserRepo) GetFirstAdmin(ctx context.Context) (*service.User, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, u := range r.users {
		if u.Role == service.RoleAdmin {
			cp := *u
			return &cp, nil
		}
	}
	return nil, service.ErrUserNotFound
}

func (r *dppsAPIUserRepo) GetUserAvatar(context.Context, int64) (*service.UserAvatar, error) {
	return nil, nil
}

func (r *dppsAPIUserRepo) UpdateUserLastActiveAt(context.Context, int64, time.Time) error {
	return nil
}

func (r *dppsAPIUserRepo) AddGroupToAllowedGroups(_ context.Context, userID int64, groupID int64) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.allowedAdds = append(r.allowedAdds, strconv.FormatInt(userID, 10)+":"+strconv.FormatInt(groupID, 10))
	if u, ok := r.users[userID]; ok {
		for _, gid := range u.AllowedGroups {
			if gid == groupID {
				return nil
			}
		}
		u.AllowedGroups = append(u.AllowedGroups, groupID)
	}
	return nil
}

type dppsAPIUsageRow struct {
	AccountID           int64
	APIKeyID            int64
	InputTokens         int64
	OutputTokens        int64
	CacheCreationTokens int64
	CacheReadTokens     int64
	TotalCost           float64
	ActualCost          float64
	AccountStatsCost    float64
	CreatedAt           time.Time
}

type dppsAPIState struct {
	mu               sync.Mutex
	nextGroupID      int64
	accounts         map[int64]*service.Account
	groups           map[int64]*service.Group
	deletedGroups    map[int64]bool
	accountGroups    map[int64][]int64
	apiKeys          map[int64]*service.APIKey
	usageRows        []dppsAPIUsageRow
	outboxEvents     []map[string]any
	invalidatedKeys  []string
	mutateCount      int
	userRepo         *dppsAPIUserRepo
	strictRejectKeys bool
}

func newDppsAPIState() *dppsAPIState {
	return &dppsAPIState{
		nextGroupID:   100,
		accounts:      make(map[int64]*service.Account),
		groups:        make(map[int64]*service.Group),
		deletedGroups: make(map[int64]bool),
		accountGroups: make(map[int64][]int64),
		apiKeys:       make(map[int64]*service.APIKey),
		userRepo:      newDppsAPIUserRepo(),
	}
}

func (s *dppsAPIState) addGroup(id int64, name string) *service.Group {
	s.mu.Lock()
	defer s.mu.Unlock()
	g := &service.Group{
		ID:                    id,
		Name:                  name,
		Platform:              service.PlatformOpenAI,
		RateMultiplier:        1.0,
		IsExclusive:           false,
		Status:                service.StatusActive,
		SubscriptionType:      service.SubscriptionTypeStandard,
		AllowMessagesDispatch: true,
		ClaudeCodeOnly:        false,
	}
	s.groups[id] = g
	if id >= s.nextGroupID {
		s.nextGroupID = id + 1
	}
	return g
}

func (s *dppsAPIState) seatGroupsForAccountLocked(accountID int64) []service.DppsSeatGroupRef {
	ids := s.accountGroups[accountID]
	out := make([]service.DppsSeatGroupRef, 0, len(ids))
	for _, gid := range ids {
		if s.deletedGroups[gid] {
			continue
		}
		if g, ok := s.groups[gid]; ok {
			out = append(out, service.DppsSeatGroupRef{ID: g.ID, Name: g.Name})
		}
	}
	return out
}

func (s *dppsAPIState) hydrateAccountLocked(acc *service.Account) service.Account {
	cp := *acc
	ids := s.accountGroups[acc.ID]
	cp.GroupIDs = make([]int64, 0, len(ids))
	cp.Groups = make([]*service.Group, 0, len(ids))
	cp.AccountGroups = make([]service.AccountGroup, 0, len(ids))
	for i, gid := range ids {
		if s.deletedGroups[gid] {
			continue
		}
		g := s.groups[gid]
		if g == nil {
			continue
		}
		gcp := *g
		cp.GroupIDs = append(cp.GroupIDs, gid)
		cp.Groups = append(cp.Groups, &gcp)
		cp.AccountGroups = append(cp.AccountGroups, service.AccountGroup{
			AccountID: acc.ID,
			GroupID:   gid,
			Priority:  i + 1,
			Group:     &gcp,
		})
	}
	return cp
}

type dppsAPISeatRepo struct{ st *dppsAPIState }

func (r *dppsAPISeatRepo) ListOpenAIOAuthSeats(context.Context) ([]service.Account, error) {
	r.st.mu.Lock()
	defer r.st.mu.Unlock()
	ids := make([]int64, 0, len(r.st.accounts))
	for id := range r.st.accounts {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	out := make([]service.Account, 0, len(ids))
	for _, id := range ids {
		acc := r.st.accounts[id]
		if acc.Platform != service.PlatformOpenAI || acc.Type != service.AccountTypeOAuth {
			continue
		}
		out = append(out, r.st.hydrateAccountLocked(acc))
	}
	return out, nil
}

func (r *dppsAPISeatRepo) GetOpenAIOAuthSeatByID(_ context.Context, accountID int64) (*service.Account, error) {
	r.st.mu.Lock()
	defer r.st.mu.Unlock()
	acc, ok := r.st.accounts[accountID]
	if !ok || acc.Platform != service.PlatformOpenAI || acc.Type != service.AccountTypeOAuth {
		return nil, service.ErrDppsSeatNotFound
	}
	hydrated := r.st.hydrateAccountLocked(acc)
	return &hydrated, nil
}

func (r *dppsAPISeatRepo) GetActiveGroupByName(_ context.Context, name string) (*service.Group, error) {
	r.st.mu.Lock()
	defer r.st.mu.Unlock()
	for id, g := range r.st.groups {
		if !r.st.deletedGroups[id] && g.Name == name {
			cp := *g
			return &cp, nil
		}
	}
	return nil, service.ErrGroupNotFound
}

func (r *dppsAPISeatRepo) SetAccountDppsGroups(
	_ context.Context,
	accountID int64,
	groupNames []string,
	validateGroup func(*service.Group) error,
) (*service.DppsSetSeatGroupsResult, error) {
	r.st.mu.Lock()
	defer r.st.mu.Unlock()

	acc, ok := r.st.accounts[accountID]
	if !ok || acc.Platform != service.PlatformOpenAI || acc.Type != service.AccountTypeOAuth {
		return nil, service.ErrDppsSeatNotFound
	}

	targetGroups := make([]*service.Group, 0, len(groupNames))
	for _, name := range groupNames {
		var found *service.Group
		for id, g := range r.st.groups {
			if !r.st.deletedGroups[id] && g.Name == name {
				found = g
				break
			}
		}
		if found == nil {
			return nil, service.ErrGroupNotFound
		}
		if validateGroup != nil {
			cp := *found
			if err := validateGroup(&cp); err != nil {
				return nil, err
			}
		}
		targetGroups = append(targetGroups, found)
	}

	oldIDs := append([]int64(nil), r.st.accountGroups[accountID]...)
	nonDpps := make([]*service.Group, 0)
	for _, gid := range oldIDs {
		if r.st.deletedGroups[gid] {
			continue
		}
		if g := r.st.groups[gid]; g != nil && !strings.HasPrefix(g.Name, "dpps-") {
			nonDpps = append(nonDpps, g)
		}
	}

	finalIDs := make([]int64, 0, len(nonDpps)+len(targetGroups))
	finalRefs := make([]service.DppsSeatGroupRef, 0, len(nonDpps)+len(targetGroups))
	dppsNames := make([]string, 0, len(targetGroups))
	seen := make(map[int64]struct{})
	for _, g := range nonDpps {
		if _, dup := seen[g.ID]; dup {
			continue
		}
		seen[g.ID] = struct{}{}
		finalIDs = append(finalIDs, g.ID)
		finalRefs = append(finalRefs, service.DppsSeatGroupRef{ID: g.ID, Name: g.Name})
	}
	for _, g := range targetGroups {
		if _, dup := seen[g.ID]; dup {
			continue
		}
		seen[g.ID] = struct{}{}
		finalIDs = append(finalIDs, g.ID)
		finalRefs = append(finalRefs, service.DppsSeatGroupRef{ID: g.ID, Name: g.Name})
		dppsNames = append(dppsNames, g.Name)
	}
	sort.Strings(dppsNames)

	r.st.accountGroups[accountID] = finalIDs
	r.st.mutateCount++
	r.st.outboxEvents = append(r.st.outboxEvents, map[string]any{
		"event":      service.SchedulerOutboxEventAccountGroupsChanged,
		"account_id": accountID,
		"old_ids":    oldIDs,
		"final_ids":  finalIDs,
	})

	return &service.DppsSetSeatGroupsResult{
		ID:         accountID,
		DppsGroups: dppsNames,
		Groups:     finalRefs,
	}, nil
}

func (r *dppsAPISeatRepo) GetUsageWindow(_ context.Context, accountID int64, since, until time.Time) ([]service.DppsUsageItem, error) {
	r.st.mu.Lock()
	defer r.st.mu.Unlock()
	type pair struct {
		accID int64
		keyID int64
	}
	sums := make(map[pair]*service.DppsUsageItem)
	for _, row := range r.st.usageRows {
		if accountID > 0 && row.AccountID != accountID {
			continue
		}
		if row.CreatedAt.Before(since) || !row.CreatedAt.Before(until) {
			continue
		}
		p := pair{accID: row.AccountID, keyID: row.APIKeyID}
		it := sums[p]
		if it == nil {
			it = &service.DppsUsageItem{AccountID: row.AccountID, APIKeyID: row.APIKeyID}
			sums[p] = it
		}
		it.Requests++
		it.InputTokens += row.InputTokens
		it.OutputTokens += row.OutputTokens
		it.CacheCreationTokens += row.CacheCreationTokens
		it.CacheReadTokens += row.CacheReadTokens
		it.TotalCost += row.TotalCost
		it.ActualCost += row.ActualCost
		it.AccountStatsCost += row.AccountStatsCost
	}
	out := make([]service.DppsUsageItem, 0, len(sums))
	for _, it := range sums {
		out = append(out, *it)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].AccountID != out[j].AccountID {
			return out[i].AccountID < out[j].AccountID
		}
		return out[i].APIKeyID < out[j].APIKeyID
	})
	return out, nil
}

type dppsAPIGroupRepo struct {
	service.AdminGroupRepository
	st *dppsAPIState
}

func (r *dppsAPIGroupRepo) Create(_ context.Context, group *service.Group) error {
	r.st.mu.Lock()
	defer r.st.mu.Unlock()
	for id, existing := range r.st.groups {
		if !r.st.deletedGroups[id] && existing.Name == group.Name {
			return service.ErrGroupExists
		}
	}
	group.ID = r.st.nextGroupID
	r.st.nextGroupID++
	cp := *group
	r.st.groups[group.ID] = &cp
	r.st.mutateCount++
	r.st.outboxEvents = append(r.st.outboxEvents, map[string]any{
		"event":    service.SchedulerOutboxEventGroupChanged,
		"group_id": group.ID,
	})
	return nil
}

func (r *dppsAPIGroupRepo) GetByID(_ context.Context, id int64) (*service.Group, error) {
	r.st.mu.Lock()
	defer r.st.mu.Unlock()
	if r.st.deletedGroups[id] {
		return nil, service.ErrGroupNotFound
	}
	g, ok := r.st.groups[id]
	if !ok {
		return nil, service.ErrGroupNotFound
	}
	cp := *g
	return &cp, nil
}

func (r *dppsAPIGroupRepo) GetByIDLite(ctx context.Context, id int64) (*service.Group, error) {
	return r.GetByID(ctx, id)
}

func (r *dppsAPIGroupRepo) DeleteCascadeIfEmpty(_ context.Context, id int64) ([]int64, error) {
	r.st.mu.Lock()
	defer r.st.mu.Unlock()
	if r.st.deletedGroups[id] || r.st.groups[id] == nil {
		return nil, service.ErrGroupNotFound
	}
	for accID, gids := range r.st.accountGroups {
		if r.st.accounts[accID] == nil {
			continue
		}
		for _, gid := range gids {
			if gid == id {
				return nil, service.ErrGroupNotEmpty
			}
		}
	}
	r.st.deletedGroups[id] = true
	for _, k := range r.st.apiKeys {
		if k.GroupID != nil && *k.GroupID == id {
			k.GroupID = nil
			k.Group = nil
		}
	}
	r.st.mutateCount++
	r.st.outboxEvents = append(r.st.outboxEvents, map[string]any{
		"event":    service.SchedulerOutboxEventGroupChanged,
		"group_id": id,
	})
	return nil, nil
}

type dppsAPIKeyRepo struct {
	service.APIKeyRepository
	st *dppsAPIState
}

func (r *dppsAPIKeyRepo) GetByID(_ context.Context, id int64) (*service.APIKey, error) {
	r.st.mu.Lock()
	defer r.st.mu.Unlock()
	k, ok := r.st.apiKeys[id]
	if !ok {
		return nil, service.ErrAPIKeyNotFound
	}
	cp := *k
	if k.User != nil {
		ucp := *k.User
		cp.User = &ucp
	}
	return &cp, nil
}

func (r *dppsAPIKeyRepo) GetByKey(_ context.Context, raw string) (*service.APIKey, error) {
	r.st.mu.Lock()
	defer r.st.mu.Unlock()
	for _, k := range r.st.apiKeys {
		if k.Key == raw {
			cp := *k
			if k.User != nil {
				ucp := *k.User
				cp.User = &ucp
			}
			if k.GroupID != nil && r.st.groups[*k.GroupID] != nil {
				gcp := *r.st.groups[*k.GroupID]
				cp.Group = &gcp
			}
			return &cp, nil
		}
	}
	return nil, service.ErrAPIKeyNotFound
}

func (r *dppsAPIKeyRepo) GetByKeyForAuth(ctx context.Context, raw string) (*service.APIKey, error) {
	return r.GetByKey(ctx, raw)
}

func (r *dppsAPIKeyRepo) Update(_ context.Context, key *service.APIKey, fields service.APIKeyUpdateFields) error {
	r.st.mu.Lock()
	defer r.st.mu.Unlock()
	existing, ok := r.st.apiKeys[key.ID]
	if !ok {
		return service.ErrAPIKeyNotFound
	}
	if fields.GroupID {
		existing.GroupID = key.GroupID
		existing.Group = key.Group
	}
	r.st.mutateCount++
	return nil
}

func (r *dppsAPIKeyRepo) ListKeysByGroupID(_ context.Context, groupID int64) ([]string, error) {
	r.st.mu.Lock()
	defer r.st.mu.Unlock()
	var keys []string
	for _, k := range r.st.apiKeys {
		if k.GroupID != nil && *k.GroupID == groupID {
			keys = append(keys, k.Key)
		}
	}
	sort.Strings(keys)
	return keys, nil
}

type dppsAPICacheInvalidator struct{ st *dppsAPIState }

func (c *dppsAPICacheInvalidator) InvalidateAuthCacheByKey(_ context.Context, key string) {
	c.st.mu.Lock()
	defer c.st.mu.Unlock()
	c.st.invalidatedKeys = append(c.st.invalidatedKeys, key)
}
func (c *dppsAPICacheInvalidator) InvalidateAuthCacheByUserID(context.Context, int64)  {}
func (c *dppsAPICacheInvalidator) InvalidateAuthCacheByGroupID(context.Context, int64) {}

type dppsAPIAdminServiceStub struct{ service.AdminService }

type dppsAPILogCapture struct {
	mu      sync.Mutex
	buf     bytes.Buffer
	records []string
}

func (c *dppsAPILogCapture) WriteLogEvent(ev *logger.LogEvent) {
	if ev == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if raw, err := json.Marshal(ev); err == nil {
		c.records = append(c.records, string(raw))
	}
}

func (c *dppsAPILogCapture) contents() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.buf.String() + "\n" + strings.Join(c.records, "\n")
}

func installDppsAPILogCapture(t *testing.T) *dppsAPILogCapture {
	t.Helper()
	cap := &dppsAPILogCapture{}
	prevSlog := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(&cap.buf, &slog.HandlerOptions{Level: slog.LevelDebug})))
	prevStd := log.Writer()
	log.SetOutput(&cap.buf)
	logger.SetSink(cap)
	t.Cleanup(func() {
		slog.SetDefault(prevSlog)
		log.SetOutput(prevStd)
		logger.SetSink(nil)
	})
	return cap
}

func seedSeatTokenInRepo(t *testing.T, repo service.SettingRepository) string {
	t.Helper()
	tokSvc := service.NewDppsSeatTokenService(repo)
	res, err := tokSvc.Create(context.Background(), 1)
	require.NoError(t, err)
	return res.Token
}

func newDppsSeatTestRouter(
	st *dppsAPIState,
	settingRepo service.SettingRepository,
	auditRepo service.AuditLogRepository,
	rdb *redis.Client,
	rlCfg middleware.DppsSeatRateLimitConfig,
) (*gin.Engine, *service.DppsSeatService, *service.DppsSeatTokenService) {
	gin.SetMode(gin.TestMode)
	seatSvc := service.NewDppsSeatService(
		&dppsAPISeatRepo{st: st},
		&dppsAPIGroupRepo{st: st},
		&dppsAPIKeyRepo{st: st},
		st.userRepo,
		&dppsAPICacheInvalidator{st: st},
	)
	tokSvc := service.NewDppsSeatTokenService(settingRepo)

	r := gin.New()
	v1 := r.Group("/api/v1")
	RegisterDppsSeatRoutesWithDeps(v1, DppsSeatRouteDeps{
		Handler:      handler.NewDppsSeatHandler(seatSvc),
		SettingRepo:  settingRepo,
		AuditLogRepo: auditRepo,
		RedisClient:  rdb,
		RateLimitCfg: rlCfg,
	})
	return r, seatSvc, tokSvc
}

func doSeatJSONReq(
	r http.Handler,
	method, path, body string,
	headers map[string]string,
	remoteAddr string,
) *httptest.ResponseRecorder {
	var reader io.Reader
	if body != "" {
		reader = strings.NewReader(body)
	}
	req := httptest.NewRequest(method, path, reader)
	if body != "" {
		req.Header.Set("Content-Type", "application/json")
	}
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	if remoteAddr != "" {
		req.RemoteAddr = remoteAddr
	}
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	return w
}

func dppsTestRandomHex(t *testing.T, byteLen int) string {
	t.Helper()
	b := make([]byte, byteLen)
	_, err := cryptorand.Read(b)
	require.NoError(t, err)
	return hex.EncodeToString(b)
}

func normalizeHeadersForCompare(h http.Header) map[string][]string {
	cp := make(map[string][]string, len(h))
	for k, vals := range h {
		cp[k] = append([]string(nil), vals...)
	}
	return cp
}

// ---- Required Gate S2 Tests (all 9 exact names, zero build tags, unconditional) ----

func TestSeatAPI_TokenRequired(t *testing.T) {
	logCap := installDppsAPILogCapture(t)
	st := newDppsAPIState()
	st.addGroup(10, "dpps-pool")
	st.accounts[1] = &service.Account{
		ID:          1,
		Platform:    service.PlatformOpenAI,
		Type:        service.AccountTypeOAuth,
		Status:      service.StatusActive,
		Schedulable: true,
		CreatedAt:   time.Date(2026, 11, 20, 9, 0, 0, 0, time.UTC),
	}

	settingRepo := newDppsAPISettingRepo()
	auditSpy := &dppsAPIAuditSpy{}
	r, _, tokSvc := newDppsSeatTestRouter(st, settingRepo, auditSpy, nil, middleware.DppsSeatRateLimitConfig{})

	validToken := seedSeatTokenInRepo(t, settingRepo)
	flipped := validToken[:len(validToken)-1]
	if validToken[len(validToken)-1] == 'a' {
		flipped += "b"
	} else {
		flipped += "a"
	}

	var baselineBody string
	var baselineHeaders map[string][]string

	assertUniform401 := func(name, method, path, body string, headers map[string]string) {
		t.Helper()
		w := doSeatJSONReq(r, method, path, body, headers, "127.0.0.1:4444")
		require.Equal(t, http.StatusUnauthorized, w.Code, "%s (%s %s): body=%s", name, method, path, w.Body.String())
		require.Equal(t, `{"code":"UNAUTHORIZED","message":"Unauthorized"}`, w.Body.String(), name)
		require.Empty(t, w.Header().Get("WWW-Authenticate"), name)
		require.Empty(t, w.Header().Get("X-Auth-Reason"), name)
		if baselineBody == "" {
			baselineBody = w.Body.String()
			baselineHeaders = normalizeHeadersForCompare(w.Header())
		} else {
			require.Equal(t, baselineBody, w.Body.String(), name)
			require.Equal(t, baselineHeaders, normalizeHeadersForCompare(w.Header()), name)
		}
	}

	for _, target := range []struct {
		method string
		path   string
		body   string
	}{
		{http.MethodGet, "/api/v1/dpps/seats", ""},
		{http.MethodPut, "/api/v1/dpps/seats/1/groups", `{"groups":["dpps-pool"]}`},
	} {
		assertUniform401("missing", target.method, target.path, target.body, nil)
		assertUniform401("empty", target.method, target.path, target.body, map[string]string{service.DppsSeatTokenHeader: ""})
		assertUniform401("leading-space", target.method, target.path, target.body, map[string]string{service.DppsSeatTokenHeader: " " + validToken})
		assertUniform401("trailing-space", target.method, target.path, target.body, map[string]string{service.DppsSeatTokenHeader: validToken + " "})
		assertUniform401("inner-space", target.method, target.path, target.body, map[string]string{service.DppsSeatTokenHeader: "dpps_seat_ abc"})
		assertUniform401("single-char-wrong", target.method, target.path, target.body, map[string]string{service.DppsSeatTokenHeader: flipped})
		assertUniform401("oversized", target.method, target.path, target.body, map[string]string{service.DppsSeatTokenHeader: "dpps_seat_" + strings.Repeat("a", 300)})
		assertUniform401("wrong-header-bearer", target.method, target.path, target.body, map[string]string{"Authorization": "Bearer " + validToken})
		assertUniform401("wrong-header-x-api-key", target.method, target.path, target.body, map[string]string{"x-api-key": validToken})
		assertUniform401("wrong-header-goog", target.method, target.path, target.body, map[string]string{"x-goog-api-key": validToken})
		assertUniform401("query-token", target.method, target.path+"?token="+validToken, target.body, nil)
		assertUniform401("query-key", target.method, target.path+"?key="+validToken, target.body, nil)

		// Multiple X-DPPS-Seat-Token headers must fail uniformly.
		req := httptest.NewRequest(target.method, target.path, strings.NewReader(target.body))
		req.Header.Add(service.DppsSeatTokenHeader, validToken)
		req.Header.Add(service.DppsSeatTokenHeader, validToken)
		wMulti := httptest.NewRecorder()
		r.ServeHTTP(wMulti, req)
		require.Equal(t, http.StatusUnauthorized, wMulti.Code)
		require.Equal(t, baselineBody, wMulti.Body.String())
		require.Equal(t, baselineHeaders, normalizeHeadersForCompare(wMulti.Header()))
	}

	// Valid token succeeds on both read and write routes.
	wOK := doSeatJSONReq(r, http.MethodGet, "/api/v1/dpps/seats", "", map[string]string{service.DppsSeatTokenHeader: validToken}, "")
	require.Equal(t, http.StatusOK, wOK.Code)
	wWriteOK := doSeatJSONReq(r, http.MethodPut, "/api/v1/dpps/seats/1/groups", `{"groups":["dpps-pool"]}`, map[string]string{service.DppsSeatTokenHeader: validToken}, "")
	require.Equal(t, http.StatusOK, wWriteOK.Code)

	// Rotating token immediately invalidates the old token and accepts the new token.
	rotated, err := tokSvc.Create(context.Background(), 1)
	require.NoError(t, err)
	assertUniform401("rotated-old-token", http.MethodGet, "/api/v1/dpps/seats", "", map[string]string{service.DppsSeatTokenHeader: validToken})
	wNewOK := doSeatJSONReq(r, http.MethodGet, "/api/v1/dpps/seats", "", map[string]string{service.DppsSeatTokenHeader: rotated.Token}, "")
	require.Equal(t, http.StatusOK, wNewOK.Code)

	// Revoking token immediately rejects subsequent requests.
	_, err = tokSvc.Revoke(context.Background())
	require.NoError(t, err)
	assertUniform401("revoked-token", http.MethodGet, "/api/v1/dpps/seats", "", map[string]string{service.DppsSeatTokenHeader: rotated.Token})

	// Missing setting / corrupt stored hash fail uniformly with 401.
	require.NoError(t, settingRepo.Set(context.Background(), service.DppsSettingKeySeatAPITokenHash, "not-valid-hex"))
	assertUniform401("corrupt-stored-hash", http.MethodGet, "/api/v1/dpps/seats", "", map[string]string{service.DppsSeatTokenHeader: rotated.Token})

	// Unexpected DB error fails closed with 500 without leaking DB error details.
	settingRepo.mu.Lock()
	settingRepo.getErr = errors.New("postgres secret-connection refused")
	settingRepo.mu.Unlock()
	w500 := doSeatJSONReq(r, http.MethodGet, "/api/v1/dpps/seats", "", map[string]string{service.DppsSeatTokenHeader: rotated.Token}, "")
	require.Equal(t, http.StatusInternalServerError, w500.Code)
	require.Equal(t, `{"code":"INTERNAL_ERROR","message":"Internal server error"}`, w500.Body.String())
	require.NotContains(t, w500.Body.String(), "postgres")
	settingRepo.mu.Lock()
	settingRepo.getErr = nil
	settingRepo.mu.Unlock()

	// Nil SettingRepo resolver also fails closed with 500.
	lazyR := gin.New()
	RegisterDppsSeatRoutesWithDeps(lazyR.Group("/api/v1"), DppsSeatRouteDeps{
		SettingRepoResolver: func() service.SettingRepository { return nil },
	})
	wNilRepo := doSeatJSONReq(lazyR, http.MethodGet, "/api/v1/dpps/seats", "", map[string]string{service.DppsSeatTokenHeader: rotated.Token}, "")
	require.Equal(t, http.StatusInternalServerError, wNilRepo.Code)

	// RegisterDppsSeatRoutes lazy service resolver returns 503 SERVICE_UNAVAILABLE when DppsSeatInstance() is nil.
	reSeededTok := seedSeatTokenInRepo(t, settingRepo)
	service.SetDppsSeatMiddlewareRepos(settingRepo, &dppsAPIAuditSpy{})
	service.SetDppsSeatInstance(nil)
	t.Cleanup(func() {
		service.SetDppsSeatMiddlewareRepos(nil, nil)
		service.SetDppsSeatInstance(nil)
	})
	unwiredR := gin.New()
	RegisterDppsSeatRoutes(unwiredR.Group("/api/v1"), nil)
	w503 := doSeatJSONReq(unwiredR, http.MethodGet, "/api/v1/dpps/seats", "", map[string]string{service.DppsSeatTokenHeader: reSeededTok}, "")
	require.Equal(t, http.StatusServiceUnavailable, w503.Code)
	var unavailBody map[string]any
	require.NoError(t, json.Unmarshal(w503.Body.Bytes(), &unavailBody))
	require.Equal(t, "SERVICE_UNAVAILABLE", unavailBody["reason"])

	// SettingService.GetAllSettings omits dpps_seat_api_* keys.
	settingSvc := service.NewSettingService(settingRepo, &config.Config{})
	allSettings, err := settingSvc.GetAllSettings(context.Background())
	require.NoError(t, err)
	settingsJSON, err := json.Marshal(allSettings)
	require.NoError(t, err)
	require.NotContains(t, string(settingsJSON), validToken)
	require.NotContains(t, string(settingsJSON), rotated.Token)
	require.NotContains(t, string(settingsJSON), reSeededTok)
	require.NotContains(t, string(settingsJSON), "dpps_seat_api")

	// Captured logs never contain plaintext tokens.
	logs := logCap.contents()
	require.NotContains(t, logs, validToken)
	require.NotContains(t, logs, rotated.Token)
	require.NotContains(t, logs, reSeededTok)
}

func buildFullIsolationRouter(
	t *testing.T,
	st *dppsAPIState,
	settingRepo *dppsAPISettingRepo,
	scopedKeys *dppsAPIScopedAdminKeyRepo,
	auditSpy *dppsAPIAuditSpy,
	cfg *config.Config,
) (*gin.Engine, *service.AuthService, *service.SettingService, *service.DppsSeatService, *service.DppsSeatTokenService) {
	t.Helper()
	gin.SetMode(gin.TestMode)

	userSvc := service.NewUserService(st.userRepo, nil, nil, nil)
	settingSvc := service.NewSettingService(settingRepo, cfg)
	settingSvc.SetScopedAdminKeyRepository(scopedKeys)
	authSvc := service.NewAuthService(nil, st.userRepo, nil, nil, cfg, settingSvc, nil, nil, nil, nil, nil, nil, nil)
	apiKeySvc := service.NewAPIKeyService(&dppsAPIKeyRepo{st: st}, st.userRepo, &dppsAPIGroupRepo{st: st}, nil, nil, nil, cfg)

	seatSvc := service.NewDppsSeatService(
		&dppsAPISeatRepo{st: st},
		&dppsAPIGroupRepo{st: st},
		&dppsAPIKeyRepo{st: st},
		st.userRepo,
		&dppsAPICacheInvalidator{st: st},
	)
	tokSvc := service.NewDppsSeatTokenService(settingRepo)

	service.SetDppsSeatMiddlewareRepos(settingRepo, auditSpy)
	service.SetDppsSeatInstance(seatSvc)
	service.SetDppsSeatTokenInstance(tokSvc)
	t.Cleanup(func() {
		_ = settingSvc.UpdateSettings(context.Background(), &service.SystemSettings{BackendModeEnabled: false})
		service.SetDppsSeatMiddlewareRepos(nil, nil)
		service.SetDppsSeatInstance(nil)
		service.SetDppsSeatTokenInstance(nil)
	})

	mrPanel, err := miniredis.Run()
	require.NoError(t, err)
	t.Cleanup(mrPanel.Close)
	rdbPanel := redis.NewClient(&redis.Options{Addr: mrPanel.Addr()})
	t.Cleanup(func() { _ = rdbPanel.Close() })

	jwtAuth := middleware.NewJWTAuthMiddleware(authSvc, userSvc, settingSvc, nil)
	optionalJWT := middleware.NewOptionalJWTAuthMiddleware(authSvc, userSvc, settingSvc, nil)
	adminAuth := middleware.NewAdminAuthMiddleware(authSvc, userSvc, settingSvc, nil)
	apiKeyAuth := middleware.NewAPIKeyAuthMiddleware(apiKeySvc, nil, cfg)
	stepUpAuth := middleware.NewStepUpAuthMiddleware(nil, userSvc, settingSvc)
	auditLog := middleware.AuditLogMiddleware(func(c *gin.Context) { c.Next() })
	panelLimiter := middleware.NewPanelRateLimiter(rdbPanel, settingSvc)

	handlers := &handler.Handlers{
		Gateway:       &handler.GatewayHandler{},
		OpenAIGateway: &handler.OpenAIGatewayHandler{},
		AsyncImage:    handler.NewAsyncImageHandler(nil, nil),
		MCPOAuth:      handler.NewMCPOAuthHandler(nil, nil, nil, nil, &config.Config{MCP: config.MCPConfig{PublicURL: "https://platform.example/api/v1/mcp"}}),
		Admin: &handler.AdminHandlers{
			Account: adminhandler.NewAccountHandler(dppsAPIAdminServiceStub{}, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil),
			Setting: adminhandler.NewSettingHandler(settingSvc, nil, nil, nil, nil, nil, nil),
		},
	}

	r := gin.New()
	RegisterCommonRoutes(r)
	v1 := r.Group("/api/v1")
	RegisterAuthRoutes(v1, handlers, jwtAuth, auditLog, nil, settingSvc, panelLimiter)
	RegisterUserRoutes(v1, handlers, jwtAuth, auditLog, settingSvc, panelLimiter)
	RegisterModelPlazaRoutes(v1, handlers, optionalJWT, settingSvc, panelLimiter)
	RegisterAdminRoutes(v1, handlers, adminAuth, auditLog, stepUpAuth, settingSvc, panelLimiter)
	RegisterMCPRoutes(v1, handlers, auditLog, stepUpAuth, settingSvc, panelLimiter, cfg.CORS.AllowedOrigins, nil)
	RegisterMCPOAuthRoutes(r, v1, handlers, nil)
	RegisterGatewayRoutes(r, handlers, apiKeyAuth, apiKeySvc, nil, nil, settingSvc, nil, cfg)
	RegisterPaymentRoutes(v1, handlers.Payment, handlers.PaymentWebhook, handlers.Admin.Payment, jwtAuth, adminAuth, auditLog, settingSvc, panelLimiter, nil)
	RegisterDppsSeatRoutes(v1, nil)

	return r, authSvc, settingSvc, seatSvc, tokSvc
}

func TestSeatAPI_TokenCannotReachOtherRoutes(t *testing.T) {
	st := newDppsAPIState()
	st.addGroup(10, "dpps-pool")
	st.accounts[1] = &service.Account{
		ID: 1, Platform: service.PlatformOpenAI, Type: service.AccountTypeOAuth,
		Status: service.StatusActive, Schedulable: true, CreatedAt: time.Now().UTC(),
	}
	st.userRepo.users[1] = &service.User{ID: 1, Email: "admin@example.com", Role: service.RoleAdmin, Status: service.StatusActive}

	settingRepo := newDppsAPISettingRepo()
	scopedKeys := newDppsAPIScopedAdminKeyRepo()
	auditSpy := &dppsAPIAuditSpy{}
	cfg := &config.Config{
		JWT:     config.JWTConfig{Secret: dppsTestRandomHex(t, 24), ExpireHour: 1},
		RunMode: config.RunModeStandard,
		Gateway: config.GatewayConfig{MaxBodySize: 1024 * 1024, TextMaxBodySize: 1024 * 1024},
	}

	r, _, settingSvc, _, _ := buildFullIsolationRouter(t, st, settingRepo, scopedKeys, auditSpy, cfg)
	require.NoError(t, settingSvc.UpdateSettings(context.Background(), &service.SystemSettings{BackendModeEnabled: true}))
	seatToken := seedSeatTokenInRepo(t, settingRepo)

	nonSeatTargets := []struct {
		method string
		path   string
		body   string
	}{
		{http.MethodGet, "/api/v1/admin/dpps/seat-token", ""},
		{http.MethodPost, "/api/v1/admin/dpps/seat-token", ""},
		{http.MethodDelete, "/api/v1/admin/dpps/seat-token", ""},
		{http.MethodGet, "/api/v1/admin/settings", ""},
		{http.MethodPut, "/api/v1/admin/settings", `{}`},
		{http.MethodGet, "/api/v1/admin/accounts", ""},
		{http.MethodGet, "/api/v1/admin/users", ""},
		{http.MethodGet, "/api/v1/user/profile", ""},
		{http.MethodGet, "/api/v1/keys", ""},
		{http.MethodGet, "/api/v1/usage", ""},
		{http.MethodPost, "/v1/responses", `{"model":"gpt-5.4","input":"hi"}`},
		{http.MethodPost, "/v1/chat/completions", `{"model":"gpt-5.4","messages":[{"role":"user","content":"hi"}]}`},
		{http.MethodPost, "/v1/messages", `{"model":"gpt-5.4","messages":[{"role":"user","content":"hi"}]}`},
		{http.MethodGet, "/api/v1/model-plaza", ""},
		{http.MethodPost, "/api/v1/mcp", `{"jsonrpc":"2.0","id":1,"method":"tools/list"}`},
		{http.MethodGet, "/api/v1/payment/config", ""},
	}

	credentialHeaders := []map[string]string{
		{service.DppsSeatTokenHeader: seatToken},
		{"Authorization": "Bearer " + seatToken},
		{"x-api-key": seatToken},
		{"x-goog-api-key": seatToken},
	}

	for _, hdr := range credentialHeaders {
		for _, target := range nonSeatTargets {
			w := doSeatJSONReq(r, target.method, target.path, target.body, hdr, "127.0.0.1:1234")
			if target.path == "/api/v1/model-plaza" && hdr["Authorization"] == "" {
				require.Equal(t, http.StatusForbidden, w.Code, "%s %s with %v: %s", target.method, target.path, hdr, w.Body.String())
			} else {
				require.Equal(t, http.StatusUnauthorized, w.Code, "%s %s with %v: %s", target.method, target.path, hdr, w.Body.String())
			}
			require.False(t, w.Code >= 200 && w.Code < 300)
		}
	}

	// Exact same token succeeds on /api/v1/dpps/seats via X-DPPS-Seat-Token.
	wSeat := doSeatJSONReq(r, http.MethodGet, "/api/v1/dpps/seats", "", map[string]string{service.DppsSeatTokenHeader: seatToken}, "")
	require.Equal(t, http.StatusOK, wSeat.Code)
}

func TestSeatAPI_UserKeyAndAdminJWTCannotReachSeatRoutes(t *testing.T) {
	st := newDppsAPIState()
	gPool := st.addGroup(10, "dpps-pool")
	st.accounts[1] = &service.Account{
		ID: 1, Platform: service.PlatformOpenAI, Type: service.AccountTypeOAuth,
		Status: service.StatusActive, Schedulable: true, CreatedAt: time.Now().UTC(),
	}
	adminUser := &service.User{ID: 1, Email: "admin@example.com", Role: service.RoleAdmin, Status: service.StatusActive, TokenVersion: 1, Concurrency: 5}
	regularUser := &service.User{ID: 2, Email: "user@example.com", Role: service.RoleUser, Status: service.StatusActive, TokenVersion: 1, Concurrency: 5}
	st.userRepo.users[1] = adminUser
	st.userRepo.users[2] = regularUser
	st.apiKeys[1] = &service.APIKey{
		ID: 1, UserID: 2, Key: "sk-user-key-valid-1234567890", Status: service.StatusAPIKeyActive,
		GroupID: &gPool.ID, Group: gPool, User: regularUser,
	}

	settingRepo := newDppsAPISettingRepo()
	legacyAdminKey := "adm_legacy_" + dppsTestRandomHex(t, 12)
	require.NoError(t, settingRepo.Set(context.Background(), service.SettingKeyAdminAPIKey, legacyAdminKey))

	scopedKeys := newDppsAPIScopedAdminKeyRepo()
	auditSpy := &dppsAPIAuditSpy{}
	cfg := &config.Config{
		JWT:     config.JWTConfig{Secret: dppsTestRandomHex(t, 24), ExpireHour: 1},
		RunMode: config.RunModeStandard,
		Gateway: config.GatewayConfig{MaxBodySize: 1024 * 1024, TextMaxBodySize: 1024 * 1024},
	}

	r, authSvc, settingSvc, _, _ := buildFullIsolationRouter(t, st, settingRepo, scopedKeys, auditSpy, cfg)
	require.NotEmpty(t, seedSeatTokenInRepo(t, settingRepo))

	_, err := settingSvc.AcceptAdminCompliance(context.Background(), service.AdminComplianceAcceptInput{
		AdminUserID: 1,
		Language:    "en",
		Phrase:      service.AdminComplianceAckPhraseEN,
	})
	require.NoError(t, err)

	userJWT, err := authSvc.GenerateToken(context.Background(), regularUser)
	require.NoError(t, err)
	adminJWT, err := authSvc.GenerateToken(context.Background(), adminUser)
	require.NoError(t, err)
	scopedKey, _, err := settingSvc.CreateScopedAdminKey(context.Background(), service.ScopedAdminKeyInput{
		Name:              "full-access-scoped",
		Enabled:           true,
		ConfirmFullAccess: true,
		Policy:            adminaccess.Policy{FullAccess: true},
	}, 1)
	require.NoError(t, err)

	seatTargets := []struct {
		method string
		path   string
		body   string
	}{
		{http.MethodGet, "/api/v1/dpps/seats", ""},
		{http.MethodPost, "/api/v1/dpps/groups", `{"name":"dpps-plan-probe"}`},
		{http.MethodPut, "/api/v1/dpps/seats/1/groups", `{"groups":["dpps-pool"]}`},
		{http.MethodPut, "/api/v1/dpps/keys/1/group", `{"group_id":10}`},
	}

	foreignCreds := []struct {
		name  string
		value string
		hdrs  []map[string]string
	}{
		{
			name:  "user_api_key",
			value: "sk-user-key-valid-1234567890",
			hdrs: []map[string]string{
				{"Authorization": "Bearer sk-user-key-valid-1234567890"},
				{"x-api-key": "sk-user-key-valid-1234567890"},
				{"x-goog-api-key": "sk-user-key-valid-1234567890"},
				{service.DppsSeatTokenHeader: "sk-user-key-valid-1234567890"},
			},
		},
		{
			name:  "user_jwt",
			value: userJWT,
			hdrs: []map[string]string{
				{"Authorization": "Bearer " + userJWT},
				{"Cookie": "auth_token=" + userJWT},
				{service.DppsSeatTokenHeader: userJWT},
			},
		},
		{
			name:  "admin_jwt",
			value: adminJWT,
			hdrs: []map[string]string{
				{"Authorization": "Bearer " + adminJWT},
				{"Cookie": "auth_token=" + adminJWT},
				{service.DppsSeatTokenHeader: adminJWT},
			},
		},
		{
			name:  "legacy_admin_api_key",
			value: legacyAdminKey,
			hdrs: []map[string]string{
				{"x-api-key": legacyAdminKey},
				{service.DppsSeatTokenHeader: legacyAdminKey},
			},
		},
		{
			name:  "scoped_admin_key",
			value: scopedKey,
			hdrs: []map[string]string{
				{"x-api-key": scopedKey},
				{service.DppsSeatTokenHeader: scopedKey},
			},
		},
	}

	for _, cred := range foreignCreds {
		for _, hdr := range cred.hdrs {
			for _, target := range seatTargets {
				w := doSeatJSONReq(r, target.method, target.path, target.body, hdr, "")
				require.Equal(t, http.StatusUnauthorized, w.Code, "%s %s %s %v", cred.name, target.method, target.path, hdr)
				require.Equal(t, `{"code":"UNAUTHORIZED","message":"Unauthorized"}`, w.Body.String())
			}
		}
	}

	require.Zero(t, st.mutateCount, "foreign credentials must never mutate DB/state")
	require.Empty(t, st.invalidatedKeys, "foreign credentials must never invalidate caches")
	require.Empty(t, auditSpy.snapshot(), "foreign 401 rejections must never insert Seat API audit rows")

	// Sanity check: human admin JWT reaches /admin/dpps/seat-token CRUD; adm_sc_* and legacy admin_api_key get 403.
	wScoped := doSeatJSONReq(r, http.MethodGet, "/api/v1/admin/dpps/seat-token", "", map[string]string{"x-api-key": scopedKey}, "")
	require.Equal(t, http.StatusForbidden, wScoped.Code)
	wLegacy := doSeatJSONReq(r, http.MethodGet, "/api/v1/admin/dpps/seat-token", "", map[string]string{"x-api-key": legacyAdminKey}, "")
	require.Equal(t, http.StatusForbidden, wLegacy.Code)

	wAdminGet := doSeatJSONReq(r, http.MethodGet, "/api/v1/admin/dpps/seat-token", "", map[string]string{"Authorization": "Bearer " + adminJWT}, "")
	require.Equal(t, http.StatusOK, wAdminGet.Code)
	require.Equal(t, "no-store", wAdminGet.Header().Get("Cache-Control"))

	wAdminPost := doSeatJSONReq(r, http.MethodPost, "/api/v1/admin/dpps/seat-token", "", map[string]string{"Authorization": "Bearer " + adminJWT}, "")
	require.Equal(t, http.StatusOK, wAdminPost.Code)
	require.Equal(t, "no-store", wAdminPost.Header().Get("Cache-Control"))
	var createEnv struct {
		Code int                               `json:"code"`
		Data service.DppsSeatTokenRotateResult `json:"data"`
	}
	require.NoError(t, json.Unmarshal(wAdminPost.Body.Bytes(), &createEnv))
	require.Regexp(t, regexp.MustCompile(`\Adpps_seat_[0-9a-f]{64}\z`), createEnv.Data.Token)

	// New token from POST /admin/dpps/seat-token works immediately on Seat API.
	wSeatAfterAdminPost := doSeatJSONReq(r, http.MethodGet, "/api/v1/dpps/seats", "", map[string]string{service.DppsSeatTokenHeader: createEnv.Data.Token}, "")
	require.Equal(t, http.StatusOK, wSeatAfterAdminPost.Code)

	// Step-up 2FA blocks POST/DELETE when enabled without step-up grant.
	require.NoError(t, settingRepo.Set(context.Background(), service.SettingKeyStepUpEnabled, "true"))
	wStepUpBlocked := doSeatJSONReq(r, http.MethodDelete, "/api/v1/admin/dpps/seat-token", "", map[string]string{"Authorization": "Bearer " + adminJWT}, "")
	require.Equal(t, http.StatusForbidden, wStepUpBlocked.Code)
	require.NoError(t, settingRepo.Set(context.Background(), service.SettingKeyStepUpEnabled, "false"))

	// Human admin DELETE revokes token immediately.
	wAdminDel := doSeatJSONReq(r, http.MethodDelete, "/api/v1/admin/dpps/seat-token", "", map[string]string{"Authorization": "Bearer " + adminJWT}, "")
	require.Equal(t, http.StatusOK, wAdminDel.Code)
	wSeatAfterAdminDel := doSeatJSONReq(r, http.MethodGet, "/api/v1/dpps/seats", "", map[string]string{service.DppsSeatTokenHeader: createEnv.Data.Token}, "")
	require.Equal(t, http.StatusUnauthorized, wSeatAfterAdminDel.Code)

	// When DppsSeatTokenInstance() is nil, admin token endpoints return 503.
	service.SetDppsSeatTokenInstance(nil)
	w503 := doSeatJSONReq(r, http.MethodGet, "/api/v1/admin/dpps/seat-token", "", map[string]string{"Authorization": "Bearer " + adminJWT}, "")
	require.Equal(t, http.StatusServiceUnavailable, w503.Code)
}

func TestSeatAPI_NeverReturnsCredentials(t *testing.T) {
	logCap := installDppsAPILogCapture(t)
	st := newDppsAPIState()
	gPool := st.addGroup(10, "dpps-pool")

	now := time.Now().UTC()
	future5h := now.Add(2 * time.Hour).Format(time.RFC3339)
	future7d := now.Add(96 * time.Hour).Format(time.RFC3339)
	autoPauseAt := now.Add(12 * time.Hour)
	secretNotes := "SENTINEL_ACCOUNT_NOTES"

	st.accounts[12] = &service.Account{
		ID:          12,
		Name:        "SENTINEL_EMAIL_NAME@vendor.secret",
		Notes:       &secretNotes,
		Platform:    service.PlatformOpenAI,
		Type:        service.AccountTypeOAuth,
		Status:      service.StatusActive,
		Schedulable: true,
		CreatedAt:   time.Date(2026, 11, 20, 9, 0, 0, 0, time.UTC),
		ExpiresAt:   &autoPauseAt,
		Credentials: map[string]any{
			"access_token":            "SENTINEL_ACCESS_TOKEN",
			"refresh_token":           "SENTINEL_REFRESH_TOKEN",
			"id_token":                "SENTINEL_ID_TOKEN",
			"email":                   "SENTINEL_CRED_EMAIL@vendor.secret",
			"password":                "SENTINEL_CRED_PASSWORD",
			"subscription_expires_at": "2026-12-20T09:00:00Z",
		},
		Extra: map[string]any{
			"email":                 "SENTINEL_EXTRA_EMAIL@vendor.secret",
			"account_name":          "SENTINEL_EXTRA_NAME",
			"session_token":         "SENTINEL_EXTRA_SESSION",
			"codex_5h_used_percent": 12.5,
			"codex_5h_reset_at":     future5h,
			"codex_7d_used_percent": 4.0,
			"codex_7d_reset_at":     future7d,
		},
		ErrorMessage: "HTTP 403 forbidden SENTINEL_RAW_UPSTREAM_TOKEN",
		Proxy: &service.Proxy{
			ID:       55,
			Host:     "SENTINEL_PROXY_HOST",
			Port:     1080,
			Username: "SENTINEL_PROXY_USER",
			Password: "SENTINEL_PROXY_PASS",
		},
	}
	st.accountGroups[12] = []int64{gPool.ID}

	// Second OpenAI OAuth account WITHOUT subscription_expires_at (must omit key).
	st.accounts[13] = &service.Account{
		ID:          13,
		Name:        "SENTINEL_SECOND_EMAIL@vendor.secret",
		Platform:    service.PlatformOpenAI,
		Type:        service.AccountTypeOAuth,
		Status:      service.StatusActive,
		Schedulable: true,
		CreatedAt:   time.Date(2026, 11, 21, 9, 0, 0, 0, time.UTC),
	}
	// Non-OpenAI account must be filtered out and answer 404 on single get.
	st.accounts[14] = &service.Account{
		ID:       14,
		Name:     "SENTINEL_ANTHROPIC_EMAIL@vendor.secret",
		Platform: service.PlatformAnthropic,
		Type:     service.AccountTypeOAuth,
		Status:   service.StatusActive,
	}

	st.userRepo.users[1] = &service.User{ID: 1, Email: "admin@example.com", Role: service.RoleAdmin, Status: service.StatusActive, TokenVersion: 1, Concurrency: 5}

	settingRepo := newDppsAPISettingRepo()
	scopedKeys := newDppsAPIScopedAdminKeyRepo()
	auditSpy := &dppsAPIAuditSpy{}
	cfg := &config.Config{
		JWT:     config.JWTConfig{Secret: dppsTestRandomHex(t, 24), ExpireHour: 1},
		RunMode: config.RunModeStandard,
		Gateway: config.GatewayConfig{MaxBodySize: 1024 * 1024, TextMaxBodySize: 1024 * 1024},
	}
	r, authSvc, settingSvc, _, _ := buildFullIsolationRouter(t, st, settingRepo, scopedKeys, auditSpy, cfg)
	_, err := settingSvc.AcceptAdminCompliance(context.Background(), service.AdminComplianceAcceptInput{
		AdminUserID: 1,
		Language:    "en",
		Phrase:      service.AdminComplianceAckPhraseEN,
	})
	require.NoError(t, err)
	adminJWT, err := authSvc.GenerateToken(context.Background(), st.userRepo.users[1])
	require.NoError(t, err)

	sentinelSeatToken := seedSeatTokenInRepo(t, settingRepo)
	sum := sha256.Sum256([]byte(sentinelSeatToken))

	wList := doSeatJSONReq(r, http.MethodGet, "/api/v1/dpps/seats", "", map[string]string{service.DppsSeatTokenHeader: sentinelSeatToken}, "")
	require.Equal(t, http.StatusOK, wList.Code)

	wOne := doSeatJSONReq(r, http.MethodGet, "/api/v1/dpps/seats/12", "", map[string]string{service.DppsSeatTokenHeader: sentinelSeatToken}, "")
	require.Equal(t, http.StatusOK, wOne.Code)

	wNonOpenAI := doSeatJSONReq(r, http.MethodGet, "/api/v1/dpps/seats/14", "", map[string]string{service.DppsSeatTokenHeader: sentinelSeatToken}, "")
	require.Equal(t, http.StatusNotFound, wNonOpenAI.Code)

	wSet := doSeatJSONReq(r, http.MethodPut, "/api/v1/dpps/seats/12/groups", `{"groups":["dpps-pool"]}`, map[string]string{service.DppsSeatTokenHeader: sentinelSeatToken}, "")
	require.Equal(t, http.StatusOK, wSet.Code)

	wTokGet := doSeatJSONReq(r, http.MethodGet, "/api/v1/admin/dpps/seat-token", "", map[string]string{"Authorization": "Bearer " + adminJWT}, "")
	require.Equal(t, http.StatusOK, wTokGet.Code)

	wSettingsGet := doSeatJSONReq(r, http.MethodGet, "/api/v1/admin/settings", "", map[string]string{"Authorization": "Bearer " + adminJWT}, "")
	require.Equal(t, http.StatusOK, wSettingsGet.Code)

	// Strict 11-field schema check on GET /seats and GET /seats/12.
	allowedSeatKeys := map[string]struct{}{
		"id": {}, "status": {}, "schedulable": {}, "groups": {},
		"used_percent_5h": {}, "reset_at_5h": {}, "used_percent_7d": {}, "reset_at_7d": {},
		"created_at": {}, "last_error_class": {}, "subscription_expires_at": {},
	}
	var listEnv struct {
		Code int `json:"code"`
		Data struct {
			Seats []map[string]any `json:"seats"`
		} `json:"data"`
	}
	require.NoError(t, json.Unmarshal(wList.Body.Bytes(), &listEnv))
	require.Len(t, listEnv.Data.Seats, 2)
	for _, seatObj := range listEnv.Data.Seats {
		for k := range seatObj {
			_, ok := allowedSeatKeys[k]
			require.True(t, ok, "unexpected JSON key %q in seat payload", k)
		}
	}
	require.Contains(t, listEnv.Data.Seats[0], "subscription_expires_at")
	require.NotContains(t, listEnv.Data.Seats[1], "subscription_expires_at", "subscription_expires_at must be omitted when absent")
	require.Equal(t, "forbidden", listEnv.Data.Seats[0]["last_error_class"])

	var oneEnv struct {
		Code int            `json:"code"`
		Data map[string]any `json:"data"`
	}
	require.NoError(t, json.Unmarshal(wOne.Body.Bytes(), &oneEnv))
	for k := range oneEnv.Data {
		_, ok := allowedSeatKeys[k]
		require.True(t, ok, "unexpected JSON key %q in single-seat payload", k)
	}

	// Zero sentinel strings anywhere in responses or logs.
	combined := strings.Join([]string{
		wList.Body.String(),
		wOne.Body.String(),
		wNonOpenAI.Body.String(),
		wSet.Body.String(),
		wTokGet.Body.String(),
		wSettingsGet.Body.String(),
		logCap.contents(),
	}, "\n")
	for _, sentinel := range []string{
		"SENTINEL_",
		"vendor.secret",
		sentinelSeatToken,
		hex.EncodeToString(sum[:]),
		"dpps_seat_api_token",
	} {
		require.NotContains(t, combined, sentinel)
	}
}

func TestSeatAPI_GroupsOnlyDppsPrefixed(t *testing.T) {
	st := newDppsAPIState()
	gDefault := &service.Group{
		ID: 1, Name: "default", Platform: service.PlatformOpenAI,
		IsExclusive: false, Status: service.StatusActive,
		SubscriptionType: service.SubscriptionTypeStandard, AllowMessagesDispatch: true,
	}
	st.groups[1] = gDefault
	gOpenAIPro := &service.Group{
		ID: 2, Name: "openai-pro", Platform: service.PlatformOpenAI,
		IsExclusive: false, Status: service.StatusActive,
		SubscriptionType: service.SubscriptionTypeStandard, AllowMessagesDispatch: true,
	}
	st.groups[2] = gOpenAIPro
	gPool := st.addGroup(10, "dpps-pool")
	gPlan := st.addGroup(11, "dpps-plan-101")
	gPlanBusy := st.addGroup(12, "dpps-plan-busy")
	gBadInvariant := &service.Group{
		ID: 13, Name: "dpps-plan-badinv", Platform: service.PlatformOpenAI,
		IsExclusive: true, Status: service.StatusActive,
		SubscriptionType: service.SubscriptionTypeStandard, AllowMessagesDispatch: false,
	}
	st.groups[13] = gBadInvariant

	st.accounts[42] = &service.Account{
		ID: 42, Platform: service.PlatformOpenAI, Type: service.AccountTypeOAuth,
		Status: service.StatusActive, Schedulable: true, CreatedAt: time.Now().UTC(),
	}
	st.accountGroups[42] = []int64{gDefault.ID, gPool.ID}
	st.accounts[43] = &service.Account{
		ID: 43, Platform: service.PlatformOpenAI, Type: service.AccountTypeOAuth,
		Status: service.StatusActive, Schedulable: true, CreatedAt: time.Now().UTC(),
	}
	st.accountGroups[43] = []int64{gPlanBusy.ID}

	st.apiKeys[700] = &service.APIKey{
		ID: 700, UserID: 2, Key: "sk-bound-to-empty-plan", Status: service.StatusAPIKeyActive,
		GroupID: &gPlan.ID,
	}

	settingRepo := newDppsAPISettingRepo()
	auditSpy := &dppsAPIAuditSpy{}
	r, _, _ := newDppsSeatTestRouter(st, settingRepo, auditSpy, nil, middleware.DppsSeatRateLimitConfig{})
	tok := seedSeatTokenInRepo(t, settingRepo)
	hdr := map[string]string{service.DppsSeatTokenHeader: tok}

	// POST /groups rejects non-dpps names and malformed JSON with 400.
	for _, bad := range []string{"vip", "dpps_bad", "", "DPPS-pool", "../dpps-x"} {
		body, _ := json.Marshal(map[string]string{"name": bad})
		w := doSeatJSONReq(r, http.MethodPost, "/api/v1/dpps/groups", string(body), hdr, "")
		require.Equal(t, http.StatusBadRequest, w.Code, "name=%q", bad)
		require.Contains(t, w.Body.String(), "INVALID_GROUP_NAME")
	}
	wUnknown := doSeatJSONReq(r, http.MethodPost, "/api/v1/dpps/groups", `{"name":"dpps-pool","extra":1}`, hdr, "")
	require.Equal(t, http.StatusBadRequest, wUnknown.Code)
	require.Contains(t, wUnknown.Body.String(), "INVALID_JSON")

	// POST /groups ensures dpps-plan-new with S0 F1 + allow_messages_dispatch=true.
	wEnsure := doSeatJSONReq(r, http.MethodPost, "/api/v1/dpps/groups", `{"name":"dpps-plan-new"}`, hdr, "")
	require.Equal(t, http.StatusOK, wEnsure.Code)
	var ensureEnv struct {
		Code int                                `json:"code"`
		Data handler.DppsEnsureGroupResponseDTO `json:"data"`
	}
	require.NoError(t, json.Unmarshal(wEnsure.Body.Bytes(), &ensureEnv))
	require.True(t, ensureEnv.Data.Created)
	require.Equal(t, service.PlatformOpenAI, ensureEnv.Data.Platform)
	require.False(t, ensureEnv.Data.IsExclusive)
	require.Equal(t, service.SubscriptionTypeStandard, ensureEnv.Data.SubscriptionType)
	require.True(t, ensureEnv.Data.AllowMessagesDispatch)
	require.False(t, ensureEnv.Data.ClaudeCodeOnly)
	require.Nil(t, ensureEnv.Data.FallbackGroupID)
	require.Nil(t, ensureEnv.Data.FallbackGroupIDOnInvalidRequest)

	// POST /groups returns 409 on invariant mismatch.
	wConflict := doSeatJSONReq(r, http.MethodPost, "/api/v1/dpps/groups", `{"name":"dpps-plan-badinv"}`, hdr, "")
	require.Equal(t, http.StatusConflict, wConflict.Code)
	require.Contains(t, wConflict.Body.String(), "GROUP_INVARIANT_CONFLICT")

	// PUT /seats/42/groups rejects ["dpps-pool", "default"] with 400 without modifying memberships.
	beforeGroups := append([]int64(nil), st.accountGroups[42]...)
	wRejectNonDpps := doSeatJSONReq(r, http.MethodPut, "/api/v1/dpps/seats/42/groups", `{"groups":["dpps-pool","default"]}`, hdr, "")
	require.Equal(t, http.StatusBadRequest, wRejectNonDpps.Code)
	require.Contains(t, wRejectNonDpps.Body.String(), "GROUP_PREFIX_REQUIRED")
	require.Equal(t, beforeGroups, st.accountGroups[42])

	// Missing/null groups returns 400 INVALID_REQUEST.
	wNullGroups := doSeatJSONReq(r, http.MethodPut, "/api/v1/dpps/seats/42/groups", `{"groups":null}`, hdr, "")
	require.Equal(t, http.StatusBadRequest, wNullGroups.Code)
	require.Contains(t, wNullGroups.Body.String(), "INVALID_REQUEST")

	// PUT /seats/42/groups replaces dpps-* while preserving "default".
	wReplace := doSeatJSONReq(r, http.MethodPut, "/api/v1/dpps/seats/42/groups", `{"groups":["dpps-pool","dpps-plan-101"]}`, hdr, "")
	require.Equal(t, http.StatusOK, wReplace.Code)
	require.Equal(t, []int64{1, 10, 11}, st.accountGroups[42])

	// PUT /seats/42/groups with {"groups": []} clears dpps-* while preserving "default" AND emitting outbox.
	outboxBefore := len(st.outboxEvents)
	wClear := doSeatJSONReq(r, http.MethodPut, "/api/v1/dpps/seats/42/groups", `{"groups":[]}`, hdr, "")
	require.Equal(t, http.StatusOK, wClear.Code)
	require.Equal(t, []int64{1}, st.accountGroups[42])
	require.Len(t, st.outboxEvents, outboxBefore+1)
	require.Equal(t, service.SchedulerOutboxEventAccountGroupsChanged, st.outboxEvents[len(st.outboxEvents)-1]["event"])

	// SQL-level proof via real repository.NewDppsSeatRepository + sqlmock:
	// clearing the last group on an account (finalGroupIDs == []) MUST still emit SchedulerOutboxEventAccountGroupsChanged!
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	client := dbent.NewClient(dbent.Driver(entsql.OpenDB(dialect.Postgres, db)))
	t.Cleanup(func() { _ = client.Close() })
	realRepo := repository.NewDppsSeatRepository(client, db, nil)
	realSvc := service.NewDppsSeatService(realRepo, nil, nil, nil, nil)
	sqlRouter := gin.New()
	RegisterDppsSeatRoutesWithDeps(sqlRouter.Group("/api/v1"), DppsSeatRouteDeps{
		Handler:      handler.NewDppsSeatHandler(realSvc),
		SettingRepo:  settingRepo,
		AuditLogRepo: auditSpy,
	})
	mock.ExpectBegin()
	mock.ExpectQuery(`SELECT platform, type\s+FROM accounts`).
		WithArgs(int64(42)).
		WillReturnRows(sqlmock.NewRows([]string{"platform", "type"}).AddRow(service.PlatformOpenAI, service.AccountTypeOAuth))
	mock.ExpectQuery(`SELECT ag\.group_id, g\.name\s+FROM account_groups`).
		WithArgs(int64(42)).
		WillReturnRows(sqlmock.NewRows([]string{"group_id", "name"}).AddRow(int64(10), "dpps-pool"))
	mock.ExpectExec(`DELETE FROM account_groups WHERE account_id = \$1`).
		WithArgs(int64(42)).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec(`INSERT INTO scheduler_outbox`).
		WithArgs(service.SchedulerOutboxEventAccountGroupsChanged, int64(42), nil, []byte(`{"group_ids":[10]}`)).
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()

	wRealClear := doSeatJSONReq(sqlRouter, http.MethodPut, "/api/v1/dpps/seats/42/groups", `{"groups":[]}`, hdr, "")
	require.Equal(t, http.StatusOK, wRealClear.Code, wRealClear.Body.String())
	require.NoError(t, mock.ExpectationsWereMet())

	// DELETE /groups/:id refuses "default", "dpps-pool" (400), non-empty dpps-plan-* (409), and deletes empty dpps-plan-*.
	wDelDefault := doSeatJSONReq(r, http.MethodDelete, "/api/v1/dpps/groups/1", "", hdr, "")
	require.Equal(t, http.StatusBadRequest, wDelDefault.Code)
	require.Contains(t, wDelDefault.Body.String(), "GROUP_NOT_DELETABLE")

	wDelPool := doSeatJSONReq(r, http.MethodDelete, "/api/v1/dpps/groups/10", "", hdr, "")
	require.Equal(t, http.StatusBadRequest, wDelPool.Code)
	require.Contains(t, wDelPool.Body.String(), "GROUP_NOT_DELETABLE")

	wDelBusy := doSeatJSONReq(r, http.MethodDelete, "/api/v1/dpps/groups/12", "", hdr, "")
	require.Equal(t, http.StatusConflict, wDelBusy.Code)
	require.Contains(t, wDelBusy.Body.String(), "GROUP_NOT_EMPTY")

	wDelEmpty := doSeatJSONReq(r, http.MethodDelete, "/api/v1/dpps/groups/11", "", hdr, "")
	require.Equal(t, http.StatusOK, wDelEmpty.Code)
	require.Contains(t, st.invalidatedKeys, "sk-bound-to-empty-plan")

	wDelAgain := doSeatJSONReq(r, http.MethodDelete, "/api/v1/dpps/groups/11", "", hdr, "")
	require.Equal(t, http.StatusNotFound, wDelAgain.Code)
}

func TestSeatAPI_SetGroupsIdempotent(t *testing.T) {
	st := newDppsAPIState()
	st.addGroup(10, "dpps-pool")
	st.addGroup(11, "dpps-plan-7")
	st.accounts[42] = &service.Account{
		ID: 42, Platform: service.PlatformOpenAI, Type: service.AccountTypeOAuth,
		Status: service.StatusActive, Schedulable: true, CreatedAt: time.Now().UTC(),
	}

	settingRepo := newDppsAPISettingRepo()
	auditSpy := &dppsAPIAuditSpy{}
	r, _, _ := newDppsSeatTestRouter(st, settingRepo, auditSpy, nil, middleware.DppsSeatRateLimitConfig{})
	tok := seedSeatTokenInRepo(t, settingRepo)
	hdr := map[string]string{service.DppsSeatTokenHeader: tok}

	w1 := doSeatJSONReq(r, http.MethodPut, "/api/v1/dpps/seats/42/groups", `{"groups":["dpps-pool","dpps-plan-7"]}`, hdr, "")
	require.Equal(t, http.StatusOK, w1.Code)
	w2 := doSeatJSONReq(r, http.MethodPut, "/api/v1/dpps/seats/42/groups", `{"groups":["dpps-pool","dpps-plan-7"]}`, hdr, "")
	require.Equal(t, http.StatusOK, w2.Code)
	wDup := doSeatJSONReq(r, http.MethodPut, "/api/v1/dpps/seats/42/groups", `{"groups":["dpps-pool","dpps-pool","dpps-plan-7"]}`, hdr, "")
	require.Equal(t, http.StatusOK, wDup.Code)

	require.Equal(t, w1.Body.String(), w2.Body.String())
	require.Equal(t, w1.Body.String(), wDup.Body.String())
	require.Equal(t, []int64{10, 11}, st.accountGroups[42], "no duplicate group memberships allowed")
}

func TestSeatAPI_UsageSumsPerAccountAndKey(t *testing.T) {
	since := time.Date(2026, 11, 22, 5, 0, 0, 0, time.UTC)
	until := time.Date(2026, 11, 22, 10, 0, 0, 0, time.UTC)

	st := newDppsAPIState()
	st.usageRows = []dppsAPIUsageRow{
		// Exact since boundary: INCLUDED.
		{AccountID: 12, APIKeyID: 500, InputTokens: 1000, OutputTokens: 200, CacheCreationTokens: 10, CacheReadTokens: 300, TotalCost: 0.04, ActualCost: 0.05, AccountStatsCost: 0.03, CreatedAt: since},
		// Inside [since, until) for key 500 on account 12.
		{AccountID: 12, APIKeyID: 500, InputTokens: 2000, OutputTokens: 400, CacheCreationTokens: 20, CacheReadTokens: 600, TotalCost: 0.08, ActualCost: 0.10, AccountStatsCost: 0.06, CreatedAt: since.Add(time.Hour)},
		// Inside [since, until) for key 501 on account 12 (must stay unmerged from key 500).
		{AccountID: 12, APIKeyID: 501, InputTokens: 500, OutputTokens: 100, CacheCreationTokens: 0, CacheReadTokens: 50, TotalCost: 0.02, ActualCost: 0.025, AccountStatsCost: 0.015, CreatedAt: since.Add(2 * time.Hour)},
		// Exact until boundary: EXCLUDED.
		{AccountID: 12, APIKeyID: 500, InputTokens: 9999, OutputTokens: 9999, CacheCreationTokens: 99, CacheReadTokens: 999, TotalCost: 9.99, ActualCost: 9.99, AccountStatsCost: 9.99, CreatedAt: until},
		// Other account 13 inside [since, until).
		{AccountID: 13, APIKeyID: 600, InputTokens: 700, OutputTokens: 150, CacheCreationTokens: 5, CacheReadTokens: 25, TotalCost: 0.03, ActualCost: 0.04, AccountStatsCost: 0.02, CreatedAt: since.Add(30 * time.Minute)},
	}

	settingRepo := newDppsAPISettingRepo()
	auditSpy := &dppsAPIAuditSpy{}
	r, _, _ := newDppsSeatTestRouter(st, settingRepo, auditSpy, nil, middleware.DppsSeatRateLimitConfig{})
	tok := seedSeatTokenInRepo(t, settingRepo)
	hdr := map[string]string{service.DppsSeatTokenHeader: tok}

	q := "since=" + since.Format(time.RFC3339) + "&until=" + until.Format(time.RFC3339)
	wFiltered := doSeatJSONReq(r, http.MethodGet, "/api/v1/dpps/usage?account_id=12&"+q, "", hdr, "")
	require.Equal(t, http.StatusOK, wFiltered.Code)

	var resp struct {
		Code int `json:"code"`
		Data struct {
			Since time.Time               `json:"since"`
			Until time.Time               `json:"until"`
			Items []service.DppsUsageItem `json:"items"`
		} `json:"data"`
	}
	require.NoError(t, json.Unmarshal(wFiltered.Body.Bytes(), &resp))
	require.Len(t, resp.Data.Items, 2)
	require.Equal(t, int64(500), resp.Data.Items[0].APIKeyID)
	require.Equal(t, int64(2), resp.Data.Items[0].Requests)
	require.Equal(t, int64(3000), resp.Data.Items[0].InputTokens)
	require.Equal(t, int64(600), resp.Data.Items[0].OutputTokens)
	require.Equal(t, int64(30), resp.Data.Items[0].CacheCreationTokens)
	require.Equal(t, int64(900), resp.Data.Items[0].CacheReadTokens)
	require.InDelta(t, 0.12, resp.Data.Items[0].TotalCost, 1e-9)
	require.InDelta(t, 0.15, resp.Data.Items[0].ActualCost, 1e-9)
	require.InDelta(t, 0.09, resp.Data.Items[0].AccountStatsCost, 1e-9)
	require.Equal(t, int64(501), resp.Data.Items[1].APIKeyID)
	require.Equal(t, int64(1), resp.Data.Items[1].Requests)

	// Unfiltered returns both account 12 keys + account 13.
	wAll := doSeatJSONReq(r, http.MethodGet, "/api/v1/dpps/usage?"+q, "", hdr, "")
	require.Equal(t, http.StatusOK, wAll.Code)
	require.NoError(t, json.Unmarshal(wAll.Body.Bytes(), &resp))
	require.Len(t, resp.Data.Items, 3)

	// Empty window returns items: [] (never null).
	wEmpty := doSeatJSONReq(r, http.MethodGet, "/api/v1/dpps/usage?account_id=999&"+q, "", hdr, "")
	require.Equal(t, http.StatusOK, wEmpty.Code)
	require.Contains(t, wEmpty.Body.String(), `"items":[]`)

	// Validation errors return 400.
	for _, badQuery := range []string{
		"",
		"since=" + since.Format(time.RFC3339),
		"since=bad&until=" + until.Format(time.RFC3339),
		"since=" + until.Format(time.RFC3339) + "&until=" + since.Format(time.RFC3339),
		"since=" + since.Format(time.RFC3339) + "&until=" + since.Format(time.RFC3339),
		"since=" + since.Format(time.RFC3339) + "&until=" + since.Add(91*24*time.Hour).Format(time.RFC3339),
		"account_id=0&" + q,
		"account_id=-5&" + q,
		"account_id=abc&" + q,
	} {
		wBad := doSeatJSONReq(r, http.MethodGet, "/api/v1/dpps/usage?"+badQuery, "", hdr, "")
		require.Equal(t, http.StatusBadRequest, wBad.Code, "query=%q", badQuery)
	}

	// Verify real SQL query via repository.NewDppsSeatRepository + sqlmock.
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	realRepo := repository.NewDppsSeatRepository(nil, db, nil)
	realSvc := service.NewDppsSeatService(realRepo, nil, nil, nil, nil)
	sqlRouter := gin.New()
	RegisterDppsSeatRoutesWithDeps(sqlRouter.Group("/api/v1"), DppsSeatRouteDeps{
		Handler:      handler.NewDppsSeatHandler(realSvc),
		SettingRepo:  settingRepo,
		AuditLogRepo: auditSpy,
	})
	rows := sqlmock.NewRows([]string{
		"account_id", "api_key_id", "requests", "input_tokens", "output_tokens",
		"cache_creation_tokens", "cache_read_tokens", "total_cost", "actual_cost", "account_stats_cost",
	}).AddRow(int64(12), int64(500), int64(14), int64(12000), int64(3400), int64(0), int64(4000), 0.1425, 0.16, 0.118)
	mock.ExpectQuery(`(?s)SELECT\s+account_id,\s+api_key_id.*FROM usage_logs\s+WHERE \(\$1::bigint = 0 OR account_id = \$1\)\s+AND created_at >= \$2\s+AND created_at < \$3\s+GROUP BY account_id, api_key_id`).
		WithArgs(int64(12), since, until).
		WillReturnRows(rows)

	wSQL := doSeatJSONReq(sqlRouter, http.MethodGet, "/api/v1/dpps/usage?account_id=12&"+q, "", hdr, "")
	require.Equal(t, http.StatusOK, wSQL.Code)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestSeatAPI_KeyBindOnlyDppsGroup(t *testing.T) {
	st := newDppsAPIState()
	gDefault := &service.Group{
		ID: 1, Name: "default", Platform: service.PlatformOpenAI,
		IsExclusive: false, Status: service.StatusActive,
		SubscriptionType: service.SubscriptionTypeStandard, AllowMessagesDispatch: true,
	}
	st.groups[1] = gDefault
	gPlan7 := st.addGroup(7, "dpps-plan-7")

	uUnrestricted := &service.User{ID: 3, RestrictPublicGroups: false}
	uRestricted := &service.User{ID: 4, RestrictPublicGroups: true}
	st.userRepo.users[3] = uUnrestricted
	st.userRepo.users[4] = uRestricted

	st.apiKeys[500] = &service.APIKey{
		ID: 500, UserID: 3, Key: "sk-secret-key-500", Status: service.StatusAPIKeyActive,
		GroupID: &gDefault.ID, User: uUnrestricted, UpdatedAt: time.Now().UTC(),
	}
	st.apiKeys[501] = &service.APIKey{
		ID: 501, UserID: 4, Key: "sk-secret-key-501", Status: service.StatusAPIKeyActive,
		User: uRestricted, UpdatedAt: time.Now().UTC(),
	}

	settingRepo := newDppsAPISettingRepo()
	auditSpy := &dppsAPIAuditSpy{}
	r, _, _ := newDppsSeatTestRouter(st, settingRepo, auditSpy, nil, middleware.DppsSeatRateLimitConfig{})
	tok := seedSeatTokenInRepo(t, settingRepo)
	hdr := map[string]string{service.DppsSeatTokenHeader: tok}

	// Non-dpps-* group rejected with 403, zero mutations or invalidations.
	wForbidden := doSeatJSONReq(r, http.MethodPut, "/api/v1/dpps/keys/500/group", `{"group_id":1}`, hdr, "")
	require.Equal(t, http.StatusForbidden, wForbidden.Code)
	require.Contains(t, wForbidden.Body.String(), "GROUP_NOT_ALLOWED")
	require.Equal(t, int64(1), *st.apiKeys[500].GroupID)
	require.Empty(t, st.invalidatedKeys)

	// Invalid group_id or unknown IDs.
	for _, body := range []string{`{"group_id":0}`, `{"group_id":-1}`, `{}`, `{"group_id":7,"unknown":1}`} {
		wBad := doSeatJSONReq(r, http.MethodPut, "/api/v1/dpps/keys/500/group", body, hdr, "")
		require.Equal(t, http.StatusBadRequest, wBad.Code, body)
	}
	wNotFoundKey := doSeatJSONReq(r, http.MethodPut, "/api/v1/dpps/keys/999/group", `{"group_id":7}`, hdr, "")
	require.Equal(t, http.StatusNotFound, wNotFoundKey.Code)
	wNotFoundGrp := doSeatJSONReq(r, http.MethodPut, "/api/v1/dpps/keys/500/group", `{"group_id":999}`, hdr, "")
	require.Equal(t, http.StatusNotFound, wNotFoundGrp.Code)

	// Unrestricted user key 500 binds to dpps-plan-7.
	wBind := doSeatJSONReq(r, http.MethodPut, "/api/v1/dpps/keys/500/group", `{"group_id":7}`, hdr, "")
	require.Equal(t, http.StatusOK, wBind.Code)
	require.Equal(t, gPlan7.ID, *st.apiKeys[500].GroupID)
	require.NotContains(t, wBind.Body.String(), "sk-secret-key-500")
	require.NotContains(t, wBind.Body.String(), `"key"`)
	require.Empty(t, st.userRepo.allowedAdds)
	require.Equal(t, []string{"sk-secret-key-500"}, st.invalidatedKeys)

	// Restricted user key 501 adds group to allowed_groups and is idempotent on repeat calls.
	wBindR1 := doSeatJSONReq(r, http.MethodPut, "/api/v1/dpps/keys/501/group", `{"group_id":7}`, hdr, "")
	require.Equal(t, http.StatusOK, wBindR1.Code)
	wBindR2 := doSeatJSONReq(r, http.MethodPut, "/api/v1/dpps/keys/501/group", `{"group_id":7}`, hdr, "")
	require.Equal(t, http.StatusOK, wBindR2.Code)
	require.Equal(t, []string{"4:7", "4:7"}, st.userRepo.allowedAdds)
	require.Equal(t, []string{"sk-secret-key-500", "sk-secret-key-501", "sk-secret-key-501"}, st.invalidatedKeys)
	require.True(t, st.userRepo.users[4].CanBindGroup(7, false))
}

func TestSeatAPI_AuditRowPerWrite(t *testing.T) {
	mr, err := miniredis.Run()
	require.NoError(t, err)
	t.Cleanup(mr.Close)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })

	st := newDppsAPIState()
	st.addGroup(1, "default")
	st.addGroup(10, "dpps-pool")
	st.addGroup(11, "dpps-plan-empty")
	st.accounts[42] = &service.Account{
		ID: 42, Platform: service.PlatformOpenAI, Type: service.AccountTypeOAuth,
		Status: service.StatusActive, Schedulable: true, CreatedAt: time.Now().UTC(),
	}
	st.apiKeys[500] = &service.APIKey{
		ID: 500, UserID: 3, Key: "sk-audit-test-secret", Status: service.StatusAPIKeyActive,
		User: &service.User{ID: 3},
	}

	settingRepo := newDppsAPISettingRepo()
	var syncInsertCalls int
	auditSpy := &dppsAPIAuditSpy{
		onCall: func(_ *service.AuditLog) {
			syncInsertCalls++
		},
	}

	rlCfg := middleware.DppsSeatRateLimitConfig{
		PreAuthPerMin: 30,
		ReadPerMin:    30,
		WritePerMin:   30,
		Window:        time.Minute,
	}
	r, _, _ := newDppsSeatTestRouter(st, settingRepo, auditSpy, rdb, rlCfg)
	rawToken := seedSeatTokenInRepo(t, settingRepo)
	sum := sha256.Sum256([]byte(rawToken))
	wantMasked := "sha256:" + hex.EncodeToString(sum[:])[:12]

	hdr := map[string]string{
		service.DppsSeatTokenHeader: rawToken,
		"X-Request-ID":              "req-audit-1",
	}

	// 1. Authenticated GET -> 0 audit rows.
	wGet := doSeatJSONReq(r, http.MethodGet, "/api/v1/dpps/seats", "", hdr, "127.0.0.1:1001")
	require.Equal(t, http.StatusOK, wGet.Code)
	require.Empty(t, auditSpy.snapshot())

	// 2. Unauthenticated POST -> 0 audit rows.
	w401 := doSeatJSONReq(r, http.MethodPost, "/api/v1/dpps/groups", `{"name":"dpps-plan-nope"}`, nil, "127.0.0.1:1001")
	require.Equal(t, http.StatusUnauthorized, w401.Code)
	require.Empty(t, auditSpy.snapshot())

	// 3-8. Six authenticated writes (incl. idempotent repeat and authenticated 400).
	writes := []struct {
		method     string
		path       string
		body       string
		wantCode   int
		wantAction string
	}{
		{http.MethodPost, "/api/v1/dpps/groups", `{"name":"dpps-plan-new"}`, http.StatusOK, "dpps.groups.ensure"},
		{http.MethodPut, "/api/v1/dpps/seats/42/groups", `{"groups":["dpps-pool"]}`, http.StatusOK, "dpps.seats.groups.update"},
		{http.MethodPut, "/api/v1/dpps/seats/42/groups", `{"groups":["dpps-pool"]}`, http.StatusOK, "dpps.seats.groups.update"},
		{http.MethodPut, "/api/v1/dpps/seats/42/groups", `{"groups":["default"]}`, http.StatusBadRequest, "dpps.seats.groups.update"},
		{http.MethodPut, "/api/v1/dpps/keys/500/group", `{"group_id":10}`, http.StatusOK, "dpps.keys.group.bind"},
		{http.MethodDelete, "/api/v1/dpps/groups/11", "", http.StatusOK, "dpps.groups.delete"},
	}

	for i, wtc := range writes {
		w := doSeatJSONReq(r, wtc.method, wtc.path, wtc.body, hdr, "127.0.0.1:1001")
		require.Equal(t, wtc.wantCode, w.Code, "write #%d: %s", i+1, w.Body.String())
		// Synchronous guarantee: Insert already finished before ServeHTTP returned.
		require.Equal(t, i+1, syncInsertCalls, "write #%d must insert synchronously before return", i+1)
	}

	rows := auditSpy.snapshot()
	require.Len(t, rows, 6)
	for i, row := range rows {
		require.Nil(t, row.ActorUserID)
		require.Empty(t, row.ActorEmail)
		require.Equal(t, "dpps_seat_api", row.ActorRole)
		require.Equal(t, "dpps_seat_token", row.AuthMethod)
		require.Equal(t, wantMasked, row.CredentialMasked)
		require.Equal(t, writes[i].wantAction, row.Action)
		require.Equal(t, writes[i].method, row.Method)
		require.Equal(t, writes[i].path, row.Path)
		require.Equal(t, writes[i].wantCode, row.StatusCode)
		require.Equal(t, "req-audit-1", row.RequestID)
		require.NotEmpty(t, row.Extra)

		serialized, err := json.Marshal(row)
		require.NoError(t, err)
		require.NotContains(t, string(serialized), rawToken)
		require.NotContains(t, string(serialized), rawToken[:14])
		require.NotContains(t, row.CredentialMasked, "dpps_seat_")
	}

	// Companion loopback rate-limit check using miniredis: loopback 127.0.0.1 & ::1 MUST NOT bypass rate limits!
	mrRL, err := miniredis.Run()
	require.NoError(t, err)
	t.Cleanup(mrRL.Close)
	rdbRL := redis.NewClient(&redis.Options{Addr: mrRL.Addr()})
	t.Cleanup(func() { _ = rdbRL.Close() })

	var authRepoCalls int
	rlSettingRepo := newDppsAPISettingRepo()
	rlSettingRepo.getHook = func(key string) {
		if key == service.DppsSettingKeySeatAPITokenHash {
			authRepoCalls++
		}
	}
	rlTok := seedSeatTokenInRepo(t, rlSettingRepo)
	rlRouter, _, _ := newDppsSeatTestRouter(st, rlSettingRepo, &dppsAPIAuditSpy{}, rdbRL, middleware.DppsSeatRateLimitConfig{
		PreAuthPerMin: 5,
		ReadPerMin:    2,
		WritePerMin:   1,
		Window:        time.Minute,
	})
	rlHdr := map[string]string{service.DppsSeatTokenHeader: rlTok}

	// Post-auth write limiter on 127.0.0.1: write #1 = 200, write #2 = 429 RATE_LIMITED + Retry-After.
	wW1 := doSeatJSONReq(rlRouter, http.MethodPost, "/api/v1/dpps/groups", `{"name":"dpps-pool"}`, rlHdr, "127.0.0.1:9000")
	require.Equal(t, http.StatusOK, wW1.Code)
	wW2 := doSeatJSONReq(rlRouter, http.MethodPost, "/api/v1/dpps/groups", `{"name":"dpps-pool"}`, rlHdr, "127.0.0.1:9000")
	require.Equal(t, http.StatusTooManyRequests, wW2.Code)
	require.Equal(t, `{"code":"RATE_LIMITED","message":"Too many requests, please slow down and try again later"}`, wW2.Body.String())
	retryAfter, convErr := strconv.Atoi(wW2.Header().Get("Retry-After"))
	require.NoError(t, convErr)
	require.GreaterOrEqual(t, retryAfter, 1)

	// Post-auth read limiter on 127.0.0.1: reads #1..2 = 200, read #3 = 429 RATE_LIMITED.
	require.Equal(t, http.StatusOK, doSeatJSONReq(rlRouter, http.MethodGet, "/api/v1/dpps/seats", "", rlHdr, "127.0.0.1:9000").Code)
	require.Equal(t, http.StatusOK, doSeatJSONReq(rlRouter, http.MethodGet, "/api/v1/dpps/seats", "", rlHdr, "127.0.0.1:9000").Code)
	wR3 := doSeatJSONReq(rlRouter, http.MethodGet, "/api/v1/dpps/seats", "", rlHdr, "127.0.0.1:9000")
	require.Equal(t, http.StatusTooManyRequests, wR3.Code)
	require.NotEmpty(t, wR3.Header().Get("Retry-After"))

	// Pre-auth IP limiter on IPv6 loopback [::1]: shields DB auth lookups after 5 calls.
	authCallsBefore := authRepoCalls
	invalidHdr := map[string]string{service.DppsSeatTokenHeader: "dpps_seat_invalid_flood"}
	for i := 0; i < 5; i++ {
		wIPv6 := doSeatJSONReq(rlRouter, http.MethodGet, "/api/v1/dpps/seats", "", invalidHdr, "[::1]:5000")
		require.Equal(t, http.StatusUnauthorized, wIPv6.Code)
	}
	require.Equal(t, authCallsBefore+5, authRepoCalls)

	wIPv6Blocked := doSeatJSONReq(rlRouter, http.MethodGet, "/api/v1/dpps/seats", "", invalidHdr, "[::1]:5000")
	require.Equal(t, http.StatusTooManyRequests, wIPv6Blocked.Code)
	require.NotEmpty(t, wIPv6Blocked.Header().Get("Retry-After"))
	require.Equal(t, authCallsBefore+5, authRepoCalls, "pre-auth limiter must block before auth DB lookup")
}
