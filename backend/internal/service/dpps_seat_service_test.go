package service

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/Wei-Shaw/sub2api/internal/pkg/xai"
	"github.com/stretchr/testify/require"
)

type dppsSeatTestRepo struct {
	listFn       func(ctx context.Context) ([]Account, error)
	getSeatFn    func(ctx context.Context, accountID int64) (*Account, error)
	getGroupFn   func(ctx context.Context, name string) (*Group, error)
	setGroupsFn  func(ctx context.Context, accountID int64, groupNames []string, validateGroup func(*Group) error) (*DppsSetSeatGroupsResult, error)
	getUsageFn   func(ctx context.Context, accountID int64, since, until time.Time) ([]DppsUsageItem, error)
	setCallCount int
}

func (r *dppsSeatTestRepo) ListOpenAIOAuthSeats(ctx context.Context) ([]Account, error) {
	return r.listFn(ctx)
}

func (r *dppsSeatTestRepo) GetOpenAIOAuthSeatByID(ctx context.Context, accountID int64) (*Account, error) {
	return r.getSeatFn(ctx, accountID)
}

func (r *dppsSeatTestRepo) GetActiveGroupByName(ctx context.Context, name string) (*Group, error) {
	return r.getGroupFn(ctx, name)
}

func (r *dppsSeatTestRepo) SetAccountDppsGroups(ctx context.Context, accountID int64, groupNames []string, validateGroup func(*Group) error) (*DppsSetSeatGroupsResult, error) {
	r.setCallCount++
	return r.setGroupsFn(ctx, accountID, groupNames, validateGroup)
}

func (r *dppsSeatTestRepo) GetUsageWindow(ctx context.Context, accountID int64, since, until time.Time) ([]DppsUsageItem, error) {
	return r.getUsageFn(ctx, accountID, since, until)
}

type dppsSeatTestGroupRepo struct {
	AdminGroupRepository
	createFn     func(ctx context.Context, group *Group) error
	getByIDFn    func(ctx context.Context, id int64) (*Group, error)
	getByIDLitFn func(ctx context.Context, id int64) (*Group, error)
	delEmptyFn   func(ctx context.Context, id int64) ([]int64, error)
	delCalls     int
}

func (r *dppsSeatTestGroupRepo) Create(ctx context.Context, group *Group) error {
	return r.createFn(ctx, group)
}

func (r *dppsSeatTestGroupRepo) GetByID(ctx context.Context, id int64) (*Group, error) {
	return r.getByIDFn(ctx, id)
}

func (r *dppsSeatTestGroupRepo) GetByIDLite(ctx context.Context, id int64) (*Group, error) {
	return r.getByIDLitFn(ctx, id)
}

func (r *dppsSeatTestGroupRepo) DeleteCascadeIfEmpty(ctx context.Context, id int64) ([]int64, error) {
	r.delCalls++
	return r.delEmptyFn(ctx, id)
}

type dppsSeatTestAPIKeyRepo struct {
	APIKeyRepository
	getByIDFn     func(ctx context.Context, id int64) (*APIKey, error)
	updateFn      func(ctx context.Context, key *APIKey, fields APIKeyUpdateFields) error
	listByGroupFn func(ctx context.Context, groupID int64) ([]string, error)
	updateCalls   int
}

func (r *dppsSeatTestAPIKeyRepo) GetByID(ctx context.Context, id int64) (*APIKey, error) {
	return r.getByIDFn(ctx, id)
}

func (r *dppsSeatTestAPIKeyRepo) Update(ctx context.Context, key *APIKey, fields APIKeyUpdateFields) error {
	r.updateCalls++
	return r.updateFn(ctx, key, fields)
}

func (r *dppsSeatTestAPIKeyRepo) ListKeysByGroupID(ctx context.Context, groupID int64) ([]string, error) {
	return r.listByGroupFn(ctx, groupID)
}

type dppsSeatTestUserRepo struct {
	UserRepository
	addAllowedCalls []struct {
		userID  int64
		groupID int64
	}
	addAllowedErr error
}

func (r *dppsSeatTestUserRepo) AddGroupToAllowedGroups(_ context.Context, userID int64, groupID int64) error {
	r.addAllowedCalls = append(r.addAllowedCalls, struct {
		userID  int64
		groupID int64
	}{userID: userID, groupID: groupID})
	return r.addAllowedErr
}

type dppsSeatTestCacheInvalidator struct {
	keys []string
}

func (c *dppsSeatTestCacheInvalidator) InvalidateAuthCacheByKey(_ context.Context, key string) {
	c.keys = append(c.keys, key)
}
func (c *dppsSeatTestCacheInvalidator) InvalidateAuthCacheByUserID(context.Context, int64)  {}
func (c *dppsSeatTestCacheInvalidator) InvalidateAuthCacheByGroupID(context.Context, int64) {}

type dppsSeatTestSettingRepo struct {
	mu   sync.RWMutex
	data map[string]string
}

func newDppsSeatTestSettingRepo() *dppsSeatTestSettingRepo {
	return &dppsSeatTestSettingRepo{data: make(map[string]string)}
}

func (r *dppsSeatTestSettingRepo) Get(_ context.Context, key string) (*Setting, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	v, ok := r.data[key]
	if !ok {
		return nil, ErrSettingNotFound
	}
	return &Setting{Key: key, Value: v}, nil
}

func (r *dppsSeatTestSettingRepo) GetValue(_ context.Context, key string) (string, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	v, ok := r.data[key]
	if !ok {
		return "", ErrSettingNotFound
	}
	return v, nil
}

func (r *dppsSeatTestSettingRepo) Set(_ context.Context, key, value string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.data[key] = value
	return nil
}

func (r *dppsSeatTestSettingRepo) GetMultiple(_ context.Context, keys []string) (map[string]string, error) {
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

func (r *dppsSeatTestSettingRepo) SetMultiple(_ context.Context, settings map[string]string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	for k, v := range settings {
		r.data[k] = v
	}
	return nil
}

func (r *dppsSeatTestSettingRepo) GetAll(_ context.Context) (map[string]string, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make(map[string]string, len(r.data))
	for k, v := range r.data {
		out[k] = v
	}
	return out, nil
}

func (r *dppsSeatTestSettingRepo) Delete(_ context.Context, key string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.data[key]; !ok {
		return ErrSettingNotFound
	}
	delete(r.data, key)
	return nil
}

func dppsSeatValidGroup(id int64, name string) *Group {
	return &Group{
		ID:                    id,
		Name:                  name,
		Platform:              PlatformOpenAI,
		IsExclusive:           false,
		SubscriptionType:      SubscriptionTypeStandard,
		AllowMessagesDispatch: true,
		ClaudeCodeOnly:        false,
		Status:                StatusActive,
	}
}

func TestDppsSeatService_ListAndGetSeatsAllowlistAndNormalization(t *testing.T) {
	now := time.Date(2026, 11, 22, 12, 0, 0, 0, time.UTC)
	future5h := now.Add(2 * time.Hour).Format(time.RFC3339)
	future7d := now.Add(72 * time.Hour).Format(time.RFC3339)
	past5h := now.Add(-15 * time.Minute).Format(time.RFC3339)
	autoPause := now.Add(24 * time.Hour)
	notesSecret := "CANARY-NOTES-SECRET"

	acc1 := Account{
		ID:          1,
		Name:        "alice@core.example",
		Notes:       &notesSecret,
		Platform:    PlatformOpenAI,
		Type:        AccountTypeOAuth,
		Status:      StatusActive,
		Schedulable: true,
		CreatedAt:   now.Add(-48 * time.Hour),
		ExpiresAt:   &autoPause,
		Credentials: map[string]any{
			"access_token":            "CANARY-ACCESS-TOKEN",
			"refresh_token":           "CANARY-REFRESH-TOKEN",
			"id_token":                "CANARY-ID-TOKEN",
			"email":                   "CANARY-CRED-EMAIL@secret.test",
			"password":                "CANARY-PASSWORD",
			"subscription_expires_at": "2026-12-20T09:00:00Z",
		},
		Extra: map[string]any{
			"email":                 "CANARY-EXTRA-EMAIL@secret.test",
			"account_name":          "CANARY-EXTRA-NAME",
			"session_token":         "CANARY-SESSION-TOKEN",
			"codex_5h_used_percent": 12.5,
			"codex_5h_reset_at":     future5h,
			"codex_7d_used_percent": 4.0,
			"codex_7d_reset_at":     future7d,
		},
		ErrorMessage: "HTTP 401 unauthorized sk-CANARY-RAW-ERROR",
		Proxy: &Proxy{
			ID:       99,
			Host:     "CANARY-PROXY-HOST",
			Port:     8443,
			Username: "CANARY-PROXY-USER",
			Password: "CANARY-PROXY-PASS",
		},
		AccountGroups: []AccountGroup{
			{AccountID: 1, GroupID: 10, Priority: 2, Group: &Group{ID: 10, Name: "dpps-pool"}},
			{AccountID: 1, GroupID: 11, Priority: 1, Group: &Group{ID: 11, Name: "dpps-plan-1"}},
		},
	}

	acc2 := Account{
		ID:           2,
		Platform:     PlatformOpenAI,
		Type:         AccountTypeOAuth,
		Status:       StatusDisabled,
		Schedulable:  false,
		CreatedAt:    now.Add(-24 * time.Hour),
		ErrorMessage: "   ",
		Credentials: map[string]any{
			"subscription_expires_at": "1797757200", // 2026-12-20T09:00:00Z
		},
		Extra: map[string]any{
			"codex_5h_used_percent": 88.0,
			"codex_5h_reset_at":     past5h, // elapsed -> zeroed
		},
	}

	acc3 := Account{
		ID:           3,
		Platform:     PlatformOpenAI,
		Type:         AccountTypeOAuth,
		Status:       StatusError,
		Schedulable:  false,
		CreatedAt:    now.Add(-12 * time.Hour),
		ErrorMessage: "oauth refresh failed with upstream error",
		Credentials: map[string]any{
			"subscription_expires_at": "not-a-timestamp",
		},
	}

	nonOpenAI := Account{
		ID:       4,
		Platform: PlatformAnthropic,
		Type:     AccountTypeOAuth,
		Status:   StatusActive,
	}

	apiKeyAcc := Account{
		ID:       5,
		Platform: PlatformOpenAI,
		Type:     AccountTypeAPIKey,
		Status:   StatusActive,
	}

	repo := &dppsSeatTestRepo{
		listFn: func(context.Context) ([]Account, error) {
			return []Account{acc1, acc2, acc3, nonOpenAI, apiKeyAcc}, nil
		},
		getSeatFn: func(_ context.Context, id int64) (*Account, error) {
			switch id {
			case 1:
				copyAcc := acc1
				return &copyAcc, nil
			case 4:
				copyAcc := nonOpenAI
				return &copyAcc, nil
			case 5:
				copyAcc := apiKeyAcc
				return &copyAcc, nil
			default:
				return nil, ErrDppsSeatNotFound
			}
		},
	}

	svc := NewDppsSeatService(repo, nil, nil, nil, nil)
	svc.now = func() time.Time { return now }

	seats, err := svc.ListSeats(context.Background())
	require.NoError(t, err)
	require.Len(t, seats, 3, "non-OpenAI and non-OAuth accounts must be excluded")

	// Strict 11-field allowlist + zero canary strings.
	raw, err := json.Marshal(seats)
	require.NoError(t, err)
	rawStr := string(raw)
	for _, canary := range []string{
		"CANARY", "secret.test", "access_token", "refresh_token", "id_token",
		"session_token", "error_message", "notes", "proxy", "expires_at\":\"" + autoPause.Format(time.RFC3339),
	} {
		require.NotContains(t, rawStr, canary)
	}

	require.Equal(t, "alice@core.example", seats[0].Name, "D-303: seat carries accounts.name")

	var decoded []map[string]any
	require.NoError(t, json.Unmarshal(raw, &decoded))
	allowedKeys := map[string]struct{}{
		"id": {}, "name": {}, "status": {}, "schedulable": {}, "groups": {},
		"used_percent_5h": {}, "reset_at_5h": {}, "used_percent_7d": {}, "reset_at_7d": {},
		"created_at": {}, "last_error_class": {}, "subscription_expires_at": {},
	}
	for _, item := range decoded {
		for k := range item {
			_, ok := allowedKeys[k]
			require.True(t, ok, "unexpected seat field %q", k)
		}
	}

	// Seat 1: priority-ordered groups, 5h/7d readings, error class, subscription_expires_at.
	require.Equal(t, []DppsSeatGroupRef{{ID: 11, Name: "dpps-plan-1"}, {ID: 10, Name: "dpps-pool"}}, seats[0].Groups)
	require.NotNil(t, seats[0].UsedPercent5h)
	require.InDelta(t, 12.5, *seats[0].UsedPercent5h, 0.001)
	require.Equal(t, "auth", seats[0].LastErrorClass)
	require.NotNil(t, seats[0].SubscriptionExpiresAt)
	require.Equal(t, "2026-12-20T09:00:00Z", seats[0].SubscriptionExpiresAt.UTC().Format(time.RFC3339))

	// Seat 2: empty groups slice (non-nil), elapsed 5h zeroed to 0, nil 7d, unix timestamp parsed.
	require.NotNil(t, seats[1].Groups)
	require.Empty(t, seats[1].Groups)
	require.NotNil(t, seats[1].UsedPercent5h)
	require.Equal(t, 0.0, *seats[1].UsedPercent5h)
	require.Nil(t, seats[1].UsedPercent7d)
	require.Nil(t, seats[1].ResetAt7d)
	require.Equal(t, "", seats[1].LastErrorClass)
	require.NotNil(t, seats[1].SubscriptionExpiresAt)
	require.Equal(t, int64(1797757200), seats[1].SubscriptionExpiresAt.Unix())
	_, hasSubExp2 := decoded[1]["subscription_expires_at"]
	require.True(t, hasSubExp2)

	// Seat 3: unparseable subscription_expires_at omitted from JSON, refresh_failed class.
	require.Nil(t, seats[2].SubscriptionExpiresAt)
	_, hasSubExp3 := decoded[2]["subscription_expires_at"]
	require.False(t, hasSubExp3, "subscription_expires_at must be omitted when unparseable/absent")
	require.Equal(t, "refresh_failed", seats[2].LastErrorClass)

	// GetSeat validation and non-OpenAI/non-OAuth filtering.
	_, err = svc.GetSeat(context.Background(), 0)
	require.ErrorIs(t, err, ErrDppsInvalidAccountID)
	_, err = svc.GetSeat(context.Background(), -1)
	require.ErrorIs(t, err, ErrDppsInvalidAccountID)
	_, err = svc.GetSeat(context.Background(), 4)
	require.ErrorIs(t, err, ErrDppsSeatNotFound)
	_, err = svc.GetSeat(context.Background(), 5)
	require.ErrorIs(t, err, ErrDppsSeatNotFound)
	got1, err := svc.GetSeat(context.Background(), 1)
	require.NoError(t, err)
	require.Equal(t, int64(1), got1.ID)
}

func TestDppsSeatService_SetSeatGroupsValidationAndIdempotency(t *testing.T) {
	var capturedNames []string
	repo := &dppsSeatTestRepo{
		setGroupsFn: func(_ context.Context, accountID int64, groupNames []string, validateGroup func(*Group) error) (*DppsSetSeatGroupsResult, error) {
			capturedNames = append([]string(nil), groupNames...)
			require.NotNil(t, validateGroup)
			require.NoError(t, validateGroup(dppsSeatValidGroup(10, "dpps-pool")))
			return &DppsSetSeatGroupsResult{
				ID:         accountID,
				DppsGroups: nil, // service must normalize nil -> []
				Groups:     nil,
			}, nil
		},
	}

	svc := NewDppsSeatService(repo, nil, nil, nil, nil)

	_, err := svc.SetSeatGroups(context.Background(), 0, []string{"dpps-pool"})
	require.ErrorIs(t, err, ErrDppsInvalidAccountID)
	_, err = svc.SetSeatGroups(context.Background(), 42, nil)
	require.ErrorIs(t, err, ErrDppsInvalidRequest)

	for _, bad := range [][]string{
		{"default"},
		{"dpps-pool", "default"},
		{"DPPS-pool"},
		{"dpps_bad"},
		{""},
		{"../dpps-x"},
		{"dpps-plan-"},
		{"dpps-plan-bad/name"},
		{"dpps-plan-" + strings.Repeat("a", 91)},
	} {
		_, err := svc.SetSeatGroups(context.Background(), 42, bad)
		require.ErrorIs(t, err, ErrDppsGroupPrefixRequired, "expected rejection for %v", bad)
	}
	require.Zero(t, repo.setCallCount, "invalid group names must fail before repository mutation")

	// Deduplication in first-seen order.
	res, err := svc.SetSeatGroups(context.Background(), 42, []string{"dpps-pool", "dpps-pool", "dpps-plan-7", "dpps-pool"})
	require.NoError(t, err)
	require.Equal(t, []string{"dpps-pool", "dpps-plan-7"}, capturedNames)
	require.NotNil(t, res.DppsGroups)
	require.NotNil(t, res.Groups)

	// Empty array passes through cleanly for clearing dpps-* groups.
	res, err = svc.SetSeatGroups(context.Background(), 42, []string{})
	require.NoError(t, err)
	require.Empty(t, capturedNames)
	require.NotNil(t, res.DppsGroups)
	require.Empty(t, res.DppsGroups)

	// Validate every S0 F1 / messages-dispatch invariant.
	require.ErrorIs(t, DppsValidateGroupInvariants(nil), ErrDppsGroupInvariantConflict)
	badCases := []func(*Group){
		func(g *Group) { g.Platform = PlatformAnthropic },
		func(g *Group) { g.IsExclusive = true },
		func(g *Group) { g.SubscriptionType = SubscriptionTypeSubscription },
		func(g *Group) { id := int64(9); g.FallbackGroupID = &id },
		func(g *Group) { id := int64(9); g.FallbackGroupIDOnInvalidRequest = &id },
		func(g *Group) { g.ClaudeCodeOnly = true },
		func(g *Group) { g.Status = StatusDisabled },
		func(g *Group) { g.AllowMessagesDispatch = false },
	}
	for _, mutate := range badCases {
		g := dppsSeatValidGroup(10, "dpps-pool")
		mutate(g)
		require.ErrorIs(t, DppsValidateGroupInvariants(g), ErrDppsGroupInvariantConflict)
	}
}

func TestDppsSeatService_EnsureGroupInvariantsAndRaceRecovery(t *testing.T) {
	stored := map[string]*Group{}
	var createdGroup *Group

	seatRepo := &dppsSeatTestRepo{
		getGroupFn: func(_ context.Context, name string) (*Group, error) {
			g, ok := stored[name]
			if !ok {
				return nil, ErrGroupNotFound
			}
			cp := *g
			return &cp, nil
		},
	}
	groupRepo := &dppsSeatTestGroupRepo{
		createFn: func(_ context.Context, g *Group) error {
			if _, ok := stored[g.Name]; ok {
				return ErrGroupExists
			}
			g.ID = 101
			cp := *g
			stored[g.Name] = &cp
			createdGroup = &cp
			return nil
		},
	}

	svc := NewDppsSeatService(seatRepo, groupRepo, nil, nil, nil)

	for _, bad := range []string{"vip", "dpps_bad", "", "DPPS-pool", "../dpps-x", "dpps-plan-"} {
		_, err := svc.EnsureGroup(context.Background(), bad)
		require.ErrorIs(t, err, ErrDppsInvalidGroupName, "name=%q", bad)
	}

	// Create new group with all S0 F1 + allow_messages_dispatch invariants.
	res, err := svc.EnsureGroup(context.Background(), "dpps-plan-019352aa-1111-7000-8000-000000000001")
	require.NoError(t, err)
	require.True(t, res.Created)
	require.Equal(t, PlatformOpenAI, res.Platform)
	require.False(t, res.IsExclusive)
	require.Equal(t, SubscriptionTypeStandard, res.SubscriptionType)
	require.True(t, res.AllowMessagesDispatch)
	require.False(t, res.ClaudeCodeOnly)
	require.Nil(t, res.FallbackGroupID)
	require.Nil(t, res.FallbackGroupIDOnInvalidRequest)
	require.Equal(t, StatusActive, res.Status)
	require.NotNil(t, createdGroup)
	require.Equal(t, 1.0, createdGroup.RateMultiplier)
	require.False(t, createdGroup.RequireOAuthOnly)
	require.False(t, createdGroup.RequirePrivacySet)

	// Idempotent second call returns created=false.
	res2, err := svc.EnsureGroup(context.Background(), "dpps-plan-019352aa-1111-7000-8000-000000000001")
	require.NoError(t, err)
	require.False(t, res2.Created)
	require.Equal(t, res.ID, res2.ID)

	// Existing group with broken invariant fails closed with 409.
	badGroup := dppsSeatValidGroup(55, "dpps-pool")
	badGroup.AllowMessagesDispatch = false
	stored["dpps-pool"] = badGroup
	_, err = svc.EnsureGroup(context.Background(), "dpps-pool")
	require.ErrorIs(t, err, ErrDppsGroupInvariantConflict)

	// Concurrent race: initial GetActiveGroupByName returns ErrGroupNotFound, Create returns ErrGroupExists,
	// and re-read returns the newly committed valid group.
	raceStep := 0
	raceSeatRepo := &dppsSeatTestRepo{
		getGroupFn: func(_ context.Context, name string) (*Group, error) {
			raceStep++
			if raceStep == 1 {
				return nil, ErrGroupNotFound
			}
			return dppsSeatValidGroup(77, name), nil
		},
	}
	raceGroupRepo := &dppsSeatTestGroupRepo{
		createFn: func(context.Context, *Group) error { return ErrGroupExists },
	}
	raceSvc := NewDppsSeatService(raceSeatRepo, raceGroupRepo, nil, nil, nil)
	raceRes, err := raceSvc.EnsureGroup(context.Background(), "dpps-pool")
	require.NoError(t, err)
	require.False(t, raceRes.Created)
	require.Equal(t, int64(77), raceRes.ID)
}

func TestDppsSeatService_DeleteEmptyPlanGroup(t *testing.T) {
	groups := map[int64]*Group{
		1:  dppsSeatValidGroup(1, "default"),
		10: dppsSeatValidGroup(10, "dpps-pool"),
		11: dppsSeatValidGroup(11, "dpps-plan-busy"),
		12: dppsSeatValidGroup(12, "dpps-plan-empty"),
	}
	groupRepo := &dppsSeatTestGroupRepo{
		getByIDLitFn: func(_ context.Context, id int64) (*Group, error) {
			g, ok := groups[id]
			if !ok {
				return nil, ErrGroupNotFound
			}
			cp := *g
			return &cp, nil
		},
		delEmptyFn: func(_ context.Context, id int64) ([]int64, error) {
			if id == 11 {
				return nil, ErrGroupNotEmpty
			}
			delete(groups, id)
			return nil, nil
		},
	}
	keyRepo := &dppsSeatTestAPIKeyRepo{
		listByGroupFn: func(_ context.Context, id int64) ([]string, error) {
			if id == 12 {
				return []string{"sk-bound-1", "", "sk-bound-2"}, nil
			}
			return nil, nil
		},
	}
	inv := &dppsSeatTestCacheInvalidator{}

	svc := NewDppsSeatService(nil, groupRepo, keyRepo, nil, inv)

	_, err := svc.DeleteEmptyPlanGroup(context.Background(), 0)
	require.ErrorIs(t, err, ErrDppsInvalidGroupID)

	_, err = svc.DeleteEmptyPlanGroup(context.Background(), 999)
	require.ErrorIs(t, err, ErrGroupNotFound)

	for _, id := range []int64{1, 10} {
		_, err := svc.DeleteEmptyPlanGroup(context.Background(), id)
		require.ErrorIs(t, err, ErrDppsGroupNotDeletable)
	}
	require.Zero(t, groupRepo.delCalls)

	_, err = svc.DeleteEmptyPlanGroup(context.Background(), 11)
	require.ErrorIs(t, err, ErrGroupNotEmpty)
	require.Empty(t, inv.keys)

	res, err := svc.DeleteEmptyPlanGroup(context.Background(), 12)
	require.NoError(t, err)
	require.True(t, res.Deleted)
	require.Equal(t, int64(12), res.ID)
	require.Equal(t, "dpps-plan-empty", res.Name)
	require.Equal(t, []string{"sk-bound-1", "sk-bound-2"}, inv.keys)
}

func TestDppsSeatService_BindKeyGroup(t *testing.T) {
	now := time.Date(2026, 11, 22, 12, 30, 0, 0, time.UTC)
	groups := map[int64]*Group{
		1:  dppsSeatValidGroup(1, "default"),
		7:  dppsSeatValidGroup(7, "dpps-plan-7"),
		8:  func() *Group { g := dppsSeatValidGroup(8, "dpps-plan-8"); g.IsExclusive = true; return g }(),
		10: dppsSeatValidGroup(10, "dpps-pool"),
	}
	keys := map[int64]*APIKey{
		500: {
			ID: 500, UserID: 3, Key: "sk-key-500-secret", Status: StatusAPIKeyActive, UpdatedAt: now,
			User: &User{ID: 3, RestrictPublicGroups: false},
		},
		501: {
			ID: 501, UserID: 4, Key: "sk-key-501-secret", Status: StatusAPIKeyActive, UpdatedAt: now,
			User: &User{ID: 4, RestrictPublicGroups: true},
		},
	}

	groupRepo := &dppsSeatTestGroupRepo{
		getByIDFn: func(_ context.Context, id int64) (*Group, error) {
			g, ok := groups[id]
			if !ok {
				return nil, ErrGroupNotFound
			}
			cp := *g
			return &cp, nil
		},
	}
	var lastFields APIKeyUpdateFields
	keyRepo := &dppsSeatTestAPIKeyRepo{
		getByIDFn: func(_ context.Context, id int64) (*APIKey, error) {
			k, ok := keys[id]
			if !ok {
				return nil, ErrAPIKeyNotFound
			}
			cp := *k
			return &cp, nil
		},
		updateFn: func(_ context.Context, k *APIKey, fields APIKeyUpdateFields) error {
			lastFields = fields
			keys[k.ID].GroupID = k.GroupID
			keys[k.ID].Group = k.Group
			return nil
		},
	}
	userRepo := &dppsSeatTestUserRepo{}
	inv := &dppsSeatTestCacheInvalidator{}

	svc := NewDppsSeatService(nil, groupRepo, keyRepo, userRepo, inv)

	_, err := svc.BindKeyGroup(context.Background(), 0, 7)
	require.ErrorIs(t, err, ErrDppsInvalidKeyID)
	_, err = svc.BindKeyGroup(context.Background(), 500, 0)
	require.ErrorIs(t, err, ErrDppsInvalidGroupID)
	_, err = svc.BindKeyGroup(context.Background(), 999, 7)
	require.ErrorIs(t, err, ErrAPIKeyNotFound)
	_, err = svc.BindKeyGroup(context.Background(), 500, 999)
	require.ErrorIs(t, err, ErrGroupNotFound)

	// Non-dpps-* group rejected with 403, zero mutations.
	_, err = svc.BindKeyGroup(context.Background(), 500, 1)
	require.ErrorIs(t, err, ErrDppsGroupNotAllowed)
	_, err = svc.BindKeyGroup(context.Background(), 500, 8)
	require.ErrorIs(t, err, ErrDppsGroupInvariantConflict)
	require.Zero(t, keyRepo.updateCalls)
	require.Empty(t, inv.keys)

	// Unrestricted user: updates GroupID only, skips AddGroupToAllowedGroups, invalidates cache.
	res, err := svc.BindKeyGroup(context.Background(), 500, 7)
	require.NoError(t, err)
	require.Equal(t, int64(500), res.ID)
	require.Equal(t, int64(7), res.GroupID)
	require.Equal(t, "dpps-plan-7", res.GroupName)
	require.Equal(t, APIKeyUpdateFields{GroupID: true}, lastFields)
	require.Empty(t, userRepo.addAllowedCalls)
	require.Equal(t, []string{"sk-key-500-secret"}, inv.keys)

	// Restricted user: calls AddGroupToAllowedGroups and invalidates cache, idempotent on repeat.
	for i := 0; i < 2; i++ {
		res, err = svc.BindKeyGroup(context.Background(), 501, 7)
		require.NoError(t, err)
		require.Equal(t, int64(7), res.GroupID)
	}
	require.Len(t, userRepo.addAllowedCalls, 2)
	require.Equal(t, int64(4), userRepo.addAllowedCalls[0].userID)
	require.Equal(t, int64(7), userRepo.addAllowedCalls[0].groupID)
	require.Equal(t, []string{"sk-key-500-secret", "sk-key-501-secret", "sk-key-501-secret"}, inv.keys)
}

func TestDppsSeatService_GetUsageWindowValidationAndPassthrough(t *testing.T) {
	loc := time.FixedZone("IRST", 3*3600+30*60)
	since := time.Date(2026, 11, 22, 8, 30, 0, 0, loc)
	until := since.Add(5 * time.Hour)

	var gotAccID int64
	var gotSince, gotUntil time.Time
	repo := &dppsSeatTestRepo{
		getUsageFn: func(_ context.Context, accountID int64, s, u time.Time) ([]DppsUsageItem, error) {
			gotAccID, gotSince, gotUntil = accountID, s, u
			if accountID == 99 {
				return nil, nil
			}
			return []DppsUsageItem{{
				AccountID:        accountID,
				APIKeyID:         500,
				Requests:         3,
				TotalCost:        0.15,
				ActualCost:       0.20,
				AccountStatsCost: 0.12,
			}}, nil
		},
	}

	svc := NewDppsSeatService(repo, nil, nil, nil, nil)

	_, err := svc.GetUsageWindow(context.Background(), -1, since, until)
	require.ErrorIs(t, err, ErrDppsInvalidAccountID)
	_, err = svc.GetUsageWindow(context.Background(), 12, time.Time{}, until)
	require.ErrorIs(t, err, ErrDppsInvalidTimeRange)
	_, err = svc.GetUsageWindow(context.Background(), 12, since, since)
	require.ErrorIs(t, err, ErrDppsInvalidTimeRange)
	_, err = svc.GetUsageWindow(context.Background(), 12, until, since)
	require.ErrorIs(t, err, ErrDppsInvalidTimeRange)
	_, err = svc.GetUsageWindow(context.Background(), 12, since, since.Add(DppsUsageMaxWindow+time.Second))
	require.ErrorIs(t, err, ErrDppsWindowTooLarge)

	items, err := svc.GetUsageWindow(context.Background(), 12, since, until)
	require.NoError(t, err)
	require.Equal(t, int64(12), gotAccID)
	require.Equal(t, time.UTC, gotSince.Location())
	require.Equal(t, time.UTC, gotUntil.Location())
	require.Len(t, items, 1)
	require.Equal(t, 0.12, items[0].AccountStatsCost)

	empty, err := svc.GetUsageWindow(context.Background(), 99, since, since.Add(DppsUsageMaxWindow))
	require.NoError(t, err)
	require.NotNil(t, empty)
	require.Empty(t, empty)
}

func TestDppsSeatTokenService_HashOnlyAndSettingsIsolation(t *testing.T) {
	settingRepo := newDppsSeatTestSettingRepo()
	tokSvc := NewDppsSeatTokenService(settingRepo)
	now := time.Date(2026, 11, 22, 12, 0, 0, 0, time.UTC)
	tokSvc.now = func() time.Time { return now }

	// Unconfigured initially.
	st, err := tokSvc.GetStatus(context.Background())
	require.NoError(t, err)
	require.False(t, st.Configured)
	require.Empty(t, st.TokenPrefix)
	require.Empty(t, st.UpdatedAt)

	// Create token -> stores SHA-256 hash only, never raw token.
	created, err := tokSvc.Create(context.Background(), 1)
	require.NoError(t, err)
	require.Regexp(t, regexp.MustCompile(`\Adpps_seat_[0-9a-f]{64}\z`), created.Token)
	require.Equal(t, created.Token[:len(DppsSeatTokenPrefix)+4], created.TokenPrefix)
	require.Equal(t, now.Format(time.RFC3339), created.UpdatedAt)

	sum := sha256.Sum256([]byte(created.Token))
	wantHash := hex.EncodeToString(sum[:])

	settingRepo.mu.RLock()
	storedHash := settingRepo.data[DppsSettingKeySeatAPITokenHash]
	storedMeta := settingRepo.data[DppsSettingKeySeatAPITokenMeta]
	for k, v := range settingRepo.data {
		require.NotContains(t, v, created.Token, "raw token must never be stored under key %q", k)
	}
	settingRepo.mu.RUnlock()

	require.Equal(t, wantHash, storedHash)
	require.Contains(t, storedMeta, created.TokenPrefix)

	// Status reports configured metadata without token or hash.
	st2, err := tokSvc.GetStatus(context.Background())
	require.NoError(t, err)
	require.True(t, st2.Configured)
	require.Equal(t, created.TokenPrefix, st2.TokenPrefix)
	require.Equal(t, created.UpdatedAt, st2.UpdatedAt)

	// SettingService GetAllSettings/UpdateSettings never exposes or mutates dpps_seat_api_*.
	origXaiOpts := xai.RuntimeModelMappingOptions()
	origBackendMode := backendModeCache.Load()
	t.Cleanup(func() {
		xai.SetRuntimeModelMappingOptions(origXaiOpts)
		if origBackendMode == nil {
			backendModeCache.Store((*cachedBackendMode)(nil))
		} else {
			backendModeCache.Store(origBackendMode)
		}
	})
	settingSvc := NewSettingService(settingRepo, &config.Config{})
	allSettings, err := settingSvc.GetAllSettings(context.Background())
	require.NoError(t, err)
	settingsJSON, err := json.Marshal(allSettings)
	require.NoError(t, err)
	require.NotContains(t, string(settingsJSON), wantHash)
	require.NotContains(t, string(settingsJSON), created.Token)
	require.NotContains(t, string(settingsJSON), created.TokenPrefix)
	require.NotContains(t, string(settingsJSON), "dpps_seat_api")

	require.NoError(t, settingSvc.UpdateSettings(context.Background(), &SystemSettings{SiteName: "TestSite"}))
	gotHashAfterUpdate, err := settingRepo.GetValue(context.Background(), DppsSettingKeySeatAPITokenHash)
	require.NoError(t, err)
	require.Equal(t, wantHash, gotHashAfterUpdate)

	// dppsSettingsChangedKeys excludes dpps_seat_api_* alongside dpps_events_webhook_*.
	changed := dppsSettingsChangedKeys(
		map[string]string{
			DppsSettingKeySeatAPITokenHash:    "old",
			DppsSettingKeySeatAPITokenMeta:    "old",
			dppsSettingKeyEventsWebhookConfig: "old",
			"site_name":                       "old",
		},
		map[string]string{
			DppsSettingKeySeatAPITokenHash:    wantHash,
			DppsSettingKeySeatAPITokenMeta:    storedMeta,
			dppsSettingKeyEventsWebhookConfig: "new",
			"site_name":                       "new",
		},
	)
	require.Equal(t, []string{"site_name"}, changed)

	// Revoke removes hash & metadata.
	revoked, err := tokSvc.Revoke(context.Background())
	require.NoError(t, err)
	require.False(t, revoked.Configured)
	require.True(t, revoked.Deleted)

	st3, err := tokSvc.GetStatus(context.Background())
	require.NoError(t, err)
	require.False(t, st3.Configured)
	_, err = settingRepo.GetValue(context.Background(), DppsSettingKeySeatAPITokenHash)
	require.ErrorIs(t, err, ErrSettingNotFound)
}
