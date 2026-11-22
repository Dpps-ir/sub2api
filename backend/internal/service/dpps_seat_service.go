package service

import (
	"context"
	"errors"
	"regexp"
	"sort"
	"strings"
	"sync/atomic"
	"time"

	infraerrors "github.com/Wei-Shaw/sub2api/internal/pkg/errors"
)

const (
	// DppsPoolGroupName is the shared DPPS wallet-pool group in Sub2API.
	DppsPoolGroupName = "dpps-pool"
	// DppsPlanGroupPrefix is the required prefix for per-plan DPPS groups.
	DppsPlanGroupPrefix = "dpps-plan-"
	// DppsUsageMaxWindow bounds GET /api/v1/dpps/usage time ranges.
	DppsUsageMaxWindow = 90 * 24 * time.Hour
)

var dppsPlanGroupNameRegex = regexp.MustCompile(`\Adpps-plan-[A-Za-z0-9][A-Za-z0-9_-]{0,89}\z`)

// Seat API domain errors mapped to HTTP responses via infraerrors.
var (
	ErrDppsSeatNotFound           = infraerrors.NotFound("SEAT_NOT_FOUND", "seat not found")
	ErrDppsGroupNotAllowed        = infraerrors.Forbidden("GROUP_NOT_ALLOWED", "api key can only be bound to a dpps-* group")
	ErrDppsGroupPrefixRequired    = infraerrors.BadRequest("GROUP_PREFIX_REQUIRED", "every group name must start with dpps-")
	ErrDppsInvalidGroupName       = infraerrors.BadRequest("INVALID_GROUP_NAME", "invalid dpps group name")
	ErrDppsGroupNotDeletable      = infraerrors.BadRequest("GROUP_NOT_DELETABLE", "only empty dpps-plan-* groups can be deleted")
	ErrDppsGroupInvariantConflict = infraerrors.Conflict("GROUP_INVARIANT_CONFLICT", "group violates dpps invariants")
	ErrDppsInvalidTimeRange       = infraerrors.BadRequest("INVALID_TIME_RANGE", "until must be after since")
	ErrDppsWindowTooLarge         = infraerrors.BadRequest("WINDOW_TOO_LARGE", "time window must not exceed 90 days")
	ErrDppsInvalidAccountID       = infraerrors.BadRequest("INVALID_ACCOUNT_ID", "account id must be a positive integer")
	ErrDppsInvalidGroupID         = infraerrors.BadRequest("INVALID_GROUP_ID", "group id must be a positive integer")
	ErrDppsInvalidKeyID           = infraerrors.BadRequest("INVALID_KEY_ID", "api key id must be a positive integer")
	ErrDppsInvalidRequest         = infraerrors.BadRequest("INVALID_REQUEST", "groups must be a non-null array")
)

// DppsSeatGroupRef is the non-secret group reference embedded in seat payloads.
type DppsSeatGroupRef struct {
	ID   int64  `json:"id"`
	Name string `json:"name"`
}

// DppsSeat is the strict 11-field credential-free seat projection returned by the Seat API.
type DppsSeat struct {
	ID                    int64              `json:"id"`
	Status                string             `json:"status"`
	Schedulable           bool               `json:"schedulable"`
	Groups                []DppsSeatGroupRef `json:"groups"`
	UsedPercent5h         *float64           `json:"used_percent_5h"`
	ResetAt5h             *time.Time         `json:"reset_at_5h"`
	UsedPercent7d         *float64           `json:"used_percent_7d"`
	ResetAt7d             *time.Time         `json:"reset_at_7d"`
	CreatedAt             time.Time          `json:"created_at"`
	LastErrorClass        string             `json:"last_error_class"`
	SubscriptionExpiresAt *time.Time         `json:"subscription_expires_at,omitempty"`
}

// DppsSetSeatGroupsResult is returned by PUT /api/v1/dpps/seats/:id/groups.
type DppsSetSeatGroupsResult struct {
	ID         int64              `json:"id"`
	DppsGroups []string           `json:"dpps_groups"`
	Groups     []DppsSeatGroupRef `json:"groups"`
}

// DppsEnsureGroupResult is returned by POST /api/v1/dpps/groups.
type DppsEnsureGroupResult struct {
	ID                              int64  `json:"id"`
	Name                            string `json:"name"`
	Platform                        string `json:"platform"`
	IsExclusive                     bool   `json:"is_exclusive"`
	SubscriptionType                string `json:"subscription_type"`
	AllowMessagesDispatch           bool   `json:"allow_messages_dispatch"`
	ClaudeCodeOnly                  bool   `json:"claude_code_only"`
	FallbackGroupID                 *int64 `json:"fallback_group_id"`
	FallbackGroupIDOnInvalidRequest *int64 `json:"fallback_group_id_on_invalid_request"`
	Status                          string `json:"status"`
	Created                         bool   `json:"created"`
}

// DppsDeleteGroupResult is returned by DELETE /api/v1/dpps/groups/:id.
type DppsDeleteGroupResult struct {
	ID      int64  `json:"id"`
	Name    string `json:"name"`
	Deleted bool   `json:"deleted"`
}

// DppsBindKeyGroupResult is returned by PUT /api/v1/dpps/keys/:id/group.
type DppsBindKeyGroupResult struct {
	ID        int64     `json:"id"`
	UserID    int64     `json:"user_id"`
	GroupID   int64     `json:"group_id"`
	GroupName string    `json:"group_name"`
	Status    string    `json:"status"`
	UpdatedAt time.Time `json:"updated_at"`
}

// DppsUsageItem is one per-(account_id, api_key_id) row returned by GET /api/v1/dpps/usage.
type DppsUsageItem struct {
	AccountID           int64   `json:"account_id"`
	APIKeyID            int64   `json:"api_key_id"`
	Requests            int64   `json:"requests"`
	InputTokens         int64   `json:"input_tokens"`
	OutputTokens        int64   `json:"output_tokens"`
	CacheCreationTokens int64   `json:"cache_creation_tokens"`
	CacheReadTokens     int64   `json:"cache_read_tokens"`
	TotalCost           float64 `json:"total_cost"`
	ActualCost          float64 `json:"actual_cost"`
	AccountStatsCost    float64 `json:"account_stats_cost"`
}

// DppsSeatRepository provides Postgres persistence operations required by DppsSeatService.
type DppsSeatRepository interface {
	ListOpenAIOAuthSeats(ctx context.Context) ([]Account, error)
	GetOpenAIOAuthSeatByID(ctx context.Context, accountID int64) (*Account, error)
	GetActiveGroupByName(ctx context.Context, name string) (*Group, error)
	SetAccountDppsGroups(ctx context.Context, accountID int64, groupNames []string, validateGroup func(*Group) error) (*DppsSetSeatGroupsResult, error)
	GetUsageWindow(ctx context.Context, accountID int64, since, until time.Time) ([]DppsUsageItem, error)
}

var dppsGlobalSeatService atomic.Pointer[DppsSeatService]

// SetDppsSeatInstance registers the active DppsSeatService for route-time lookup.
func SetDppsSeatInstance(svc *DppsSeatService) {
	dppsGlobalSeatService.Store(svc)
}

// DppsSeatInstance returns the active DppsSeatService, or nil when not initialized.
func DppsSeatInstance() *DppsSeatService {
	return dppsGlobalSeatService.Load()
}

// DppsSeatService implements the 7 Seat API operations defined in docs/contracts/seat-api.md.
type DppsSeatService struct {
	seatRepo             DppsSeatRepository
	groupRepo            AdminGroupRepository
	apiKeyRepo           APIKeyRepository
	userRepo             UserRepository
	authCacheInvalidator APIKeyAuthCacheInvalidator
	now                  func() time.Time
}

// NewDppsSeatService constructs a DppsSeatService.
func NewDppsSeatService(
	seatRepo DppsSeatRepository,
	groupRepo AdminGroupRepository,
	apiKeyRepo APIKeyRepository,
	userRepo UserRepository,
	authCacheInvalidator APIKeyAuthCacheInvalidator,
) *DppsSeatService {
	return &DppsSeatService{
		seatRepo:             seatRepo,
		groupRepo:            groupRepo,
		apiKeyRepo:           apiKeyRepo,
		userRepo:             userRepo,
		authCacheInvalidator: authCacheInvalidator,
		now:                  func() time.Time { return time.Now().UTC() },
	}
}

// DppsIsValidPlanGroupName reports whether name matches ^dpps-plan-[A-Za-z0-9][A-Za-z0-9_-]{0,89}$.
func DppsIsValidPlanGroupName(name string) bool {
	return dppsPlanGroupNameRegex.MatchString(name)
}

// DppsIsValidGroupName reports whether name is "dpps-pool" or a valid "dpps-plan-*" group name.
func DppsIsValidGroupName(name string) bool {
	return name == DppsPoolGroupName || dppsPlanGroupNameRegex.MatchString(name)
}

// DppsValidateGroupInvariants verifies the S0 F1 and /v1/messages invariants on a DPPS group.
func DppsValidateGroupInvariants(group *Group) error {
	if group == nil ||
		group.Platform != PlatformOpenAI ||
		group.IsExclusive ||
		group.SubscriptionType != SubscriptionTypeStandard ||
		group.FallbackGroupID != nil ||
		group.FallbackGroupIDOnInvalidRequest != nil ||
		group.ClaudeCodeOnly ||
		group.Status != StatusActive ||
		!group.AllowMessagesDispatch {
		return ErrDppsGroupInvariantConflict
	}
	return nil
}

// ListSeats returns all non-deleted OpenAI OAuth accounts using the strict 11-field allowlist.
func (s *DppsSeatService) ListSeats(ctx context.Context) ([]DppsSeat, error) {
	if s == nil || s.seatRepo == nil {
		return nil, infraerrors.ServiceUnavailable("SEAT_SERVICE_UNAVAILABLE", "seat service unavailable")
	}
	accounts, err := s.seatRepo.ListOpenAIOAuthSeats(ctx)
	if err != nil {
		return nil, err
	}
	now := s.currentTime()
	out := make([]DppsSeat, 0, len(accounts))
	for i := range accounts {
		acc := &accounts[i]
		if acc.Platform != PlatformOpenAI || acc.Type != AccountTypeOAuth {
			continue
		}
		out = append(out, dppsAccountToSeat(acc, now))
	}
	return out, nil
}

// GetSeat returns one non-deleted OpenAI OAuth account using the strict 11-field allowlist.
func (s *DppsSeatService) GetSeat(ctx context.Context, accountID int64) (*DppsSeat, error) {
	if accountID <= 0 {
		return nil, ErrDppsInvalidAccountID
	}
	if s == nil || s.seatRepo == nil {
		return nil, infraerrors.ServiceUnavailable("SEAT_SERVICE_UNAVAILABLE", "seat service unavailable")
	}
	acc, err := s.seatRepo.GetOpenAIOAuthSeatByID(ctx, accountID)
	if err != nil {
		return nil, err
	}
	if acc == nil || acc.Platform != PlatformOpenAI || acc.Type != AccountTypeOAuth {
		return nil, ErrDppsSeatNotFound
	}
	seat := dppsAccountToSeat(acc, s.currentTime())
	return &seat, nil
}

// SetSeatGroups declaratively replaces dpps-* group memberships on accountID while preserving non-dpps-* groups.
func (s *DppsSeatService) SetSeatGroups(ctx context.Context, accountID int64, groupNames []string) (*DppsSetSeatGroupsResult, error) {
	if accountID <= 0 {
		return nil, ErrDppsInvalidAccountID
	}
	if groupNames == nil {
		return nil, ErrDppsInvalidRequest
	}
	uniqueNames := make([]string, 0, len(groupNames))
	seen := make(map[string]struct{}, len(groupNames))
	for _, name := range groupNames {
		if !DppsIsValidGroupName(name) {
			return nil, ErrDppsGroupPrefixRequired
		}
		if _, ok := seen[name]; ok {
			continue
		}
		seen[name] = struct{}{}
		uniqueNames = append(uniqueNames, name)
	}
	if s == nil || s.seatRepo == nil {
		return nil, infraerrors.ServiceUnavailable("SEAT_SERVICE_UNAVAILABLE", "seat service unavailable")
	}
	res, err := s.seatRepo.SetAccountDppsGroups(ctx, accountID, uniqueNames, DppsValidateGroupInvariants)
	if err != nil {
		return nil, err
	}
	if res.DppsGroups == nil {
		res.DppsGroups = []string{}
	}
	if res.Groups == nil {
		res.Groups = []DppsSeatGroupRef{}
	}
	return res, nil
}

// EnsureGroup idempotently creates or validates a dpps-pool or dpps-plan-* group.
func (s *DppsSeatService) EnsureGroup(ctx context.Context, name string) (*DppsEnsureGroupResult, error) {
	if !DppsIsValidGroupName(name) {
		return nil, ErrDppsInvalidGroupName
	}
	if s == nil || s.seatRepo == nil || s.groupRepo == nil {
		return nil, infraerrors.ServiceUnavailable("SEAT_SERVICE_UNAVAILABLE", "seat service unavailable")
	}

	existing, err := s.seatRepo.GetActiveGroupByName(ctx, name)
	if err == nil {
		if invErr := DppsValidateGroupInvariants(existing); invErr != nil {
			return nil, invErr
		}
		return dppsGroupToEnsureResult(existing, false), nil
	}
	if !errors.Is(err, ErrGroupNotFound) {
		return nil, err
	}

	group := &Group{
		Name:                         name,
		Platform:                     PlatformOpenAI,
		RateMultiplier:               1.0,
		IsExclusive:                  false,
		Status:                       StatusActive,
		SubscriptionType:             SubscriptionTypeStandard,
		DefaultValidityDays:          30,
		ImageRateMultiplier:          1.0,
		BatchImageDiscountMultiplier: defaultBatchImageDiscountMultiplier,
		BatchImageHoldMultiplier:     defaultBatchImageHoldMultiplier,
		VideoRateMultiplier:          1.0,
		PeakRateMultiplier:           1.0,
		LongContextPricingEnabled:    true,
		MaxReasoningEffortOverLimit:  "downgrade",
		ClaudeCodeOnly:               false,
		AllowMessagesDispatch:        true,
		RequireOAuthOnly:             false,
		RequirePrivacySet:            false,
	}
	if createErr := s.groupRepo.Create(ctx, group); createErr != nil {
		if errors.Is(createErr, ErrGroupExists) {
			reread, getErr := s.seatRepo.GetActiveGroupByName(ctx, name)
			if getErr != nil {
				return nil, getErr
			}
			if invErr := DppsValidateGroupInvariants(reread); invErr != nil {
				return nil, invErr
			}
			return dppsGroupToEnsureResult(reread, false), nil
		}
		return nil, createErr
	}
	return dppsGroupToEnsureResult(group, true), nil
}

// DeleteEmptyPlanGroup soft-deletes an empty dpps-plan-* group and invalidates bound key auth caches.
func (s *DppsSeatService) DeleteEmptyPlanGroup(ctx context.Context, groupID int64) (*DppsDeleteGroupResult, error) {
	if groupID <= 0 {
		return nil, ErrDppsInvalidGroupID
	}
	if s == nil || s.groupRepo == nil {
		return nil, infraerrors.ServiceUnavailable("SEAT_SERVICE_UNAVAILABLE", "seat service unavailable")
	}

	group, err := s.groupRepo.GetByIDLite(ctx, groupID)
	if err != nil {
		return nil, err
	}
	if !DppsIsValidPlanGroupName(group.Name) {
		return nil, ErrDppsGroupNotDeletable
	}

	var boundKeys []string
	if s.apiKeyRepo != nil {
		keys, listErr := s.apiKeyRepo.ListKeysByGroupID(ctx, groupID)
		if listErr != nil {
			return nil, listErr
		}
		boundKeys = keys
	}

	if _, err := s.groupRepo.DeleteCascadeIfEmpty(ctx, groupID); err != nil {
		return nil, err
	}

	if s.authCacheInvalidator != nil {
		for _, key := range boundKeys {
			if key == "" {
				continue
			}
			s.authCacheInvalidator.InvalidateAuthCacheByKey(ctx, key)
		}
	}

	return &DppsDeleteGroupResult{
		ID:      group.ID,
		Name:    group.Name,
		Deleted: true,
	}, nil
}

// BindKeyGroup binds an existing API key to an active dpps-* group and invalidates auth cache.
func (s *DppsSeatService) BindKeyGroup(ctx context.Context, keyID, groupID int64) (*DppsBindKeyGroupResult, error) {
	if keyID <= 0 {
		return nil, ErrDppsInvalidKeyID
	}
	if groupID <= 0 {
		return nil, ErrDppsInvalidGroupID
	}
	if s == nil || s.apiKeyRepo == nil || s.groupRepo == nil {
		return nil, infraerrors.ServiceUnavailable("SEAT_SERVICE_UNAVAILABLE", "seat service unavailable")
	}

	apiKey, err := s.apiKeyRepo.GetByID(ctx, keyID)
	if err != nil {
		return nil, err
	}
	group, err := s.groupRepo.GetByID(ctx, groupID)
	if err != nil {
		return nil, err
	}
	if !DppsIsValidGroupName(group.Name) {
		return nil, ErrDppsGroupNotAllowed
	}
	if err := DppsValidateGroupInvariants(group); err != nil {
		return nil, err
	}

	if apiKey.User != nil && apiKey.User.RestrictPublicGroups && s.userRepo != nil {
		if err := s.userRepo.AddGroupToAllowedGroups(ctx, apiKey.UserID, group.ID); err != nil {
			return nil, err
		}
	}

	gid := group.ID
	apiKey.GroupID = &gid
	apiKey.Group = group
	if err := s.apiKeyRepo.Update(ctx, apiKey, APIKeyUpdateFields{GroupID: true}); err != nil {
		return nil, err
	}

	if s.authCacheInvalidator != nil {
		s.authCacheInvalidator.InvalidateAuthCacheByKey(ctx, apiKey.Key)
	}

	updatedAt := apiKey.UpdatedAt
	if updatedAt.IsZero() {
		updatedAt = s.currentTime()
	}
	return &DppsBindKeyGroupResult{
		ID:        apiKey.ID,
		UserID:    apiKey.UserID,
		GroupID:   group.ID,
		GroupName: group.Name,
		Status:    apiKey.Status,
		UpdatedAt: updatedAt.UTC(),
	}, nil
}

// GetUsageWindow aggregates usage_logs per (account_id, api_key_id) over [since, until).
func (s *DppsSeatService) GetUsageWindow(ctx context.Context, accountID int64, since, until time.Time) ([]DppsUsageItem, error) {
	if accountID < 0 {
		return nil, ErrDppsInvalidAccountID
	}
	if since.IsZero() || until.IsZero() || !until.After(since) {
		return nil, ErrDppsInvalidTimeRange
	}
	if until.Sub(since) > DppsUsageMaxWindow {
		return nil, ErrDppsWindowTooLarge
	}
	if s == nil || s.seatRepo == nil {
		return nil, infraerrors.ServiceUnavailable("SEAT_SERVICE_UNAVAILABLE", "seat service unavailable")
	}

	items, err := s.seatRepo.GetUsageWindow(ctx, accountID, since.UTC(), until.UTC())
	if err != nil {
		return nil, err
	}
	if items == nil {
		return []DppsUsageItem{}, nil
	}
	return items, nil
}

func (s *DppsSeatService) currentTime() time.Time {
	if s != nil && s.now != nil {
		return s.now().UTC()
	}
	return time.Now().UTC()
}

func dppsGroupToEnsureResult(group *Group, created bool) *DppsEnsureGroupResult {
	return &DppsEnsureGroupResult{
		ID:                              group.ID,
		Name:                            group.Name,
		Platform:                        group.Platform,
		IsExclusive:                     group.IsExclusive,
		SubscriptionType:                group.SubscriptionType,
		AllowMessagesDispatch:           group.AllowMessagesDispatch,
		ClaudeCodeOnly:                  group.ClaudeCodeOnly,
		FallbackGroupID:                 group.FallbackGroupID,
		FallbackGroupIDOnInvalidRequest: group.FallbackGroupIDOnInvalidRequest,
		Status:                          group.Status,
		Created:                         created,
	}
}

func dppsAccountToSeat(acc *Account, now time.Time) DppsSeat {
	groups := dppsExtractSeatGroups(acc)

	var used5h *float64
	var reset5h *time.Time
	if acc.Extra != nil {
		if v, ok := acc.Extra["codex_5h_used_percent"]; ok && v != nil {
			if p := buildCodexUsageProgressFromExtra(acc.Extra, "5h", now); p != nil {
				u := p.Utilization
				used5h = &u
				if p.ResetsAt != nil {
					rt := p.ResetsAt.UTC()
					reset5h = &rt
				}
			}
		}
	}

	var used7d *float64
	var reset7d *time.Time
	if acc.Extra != nil {
		if v, ok := acc.Extra["codex_7d_used_percent"]; ok && v != nil {
			if p := buildCodexUsageProgressFromExtra(acc.Extra, "7d", now); p != nil {
				u := p.Utilization
				used7d = &u
				if p.ResetsAt != nil {
					rt := p.ResetsAt.UTC()
					reset7d = &rt
				}
			}
		}
	}

	lastErrorClass := ""
	if strings.TrimSpace(acc.ErrorMessage) != "" {
		lastErrorClass = DppsSeatErrorReasonClass(acc.ErrorMessage)
	}

	var subscriptionExpiresAt *time.Time
	if exp := acc.GetCredentialAsTime("subscription_expires_at"); exp != nil {
		expUTC := exp.UTC()
		subscriptionExpiresAt = &expUTC
	}

	return DppsSeat{
		ID:                    acc.ID,
		Status:                acc.Status,
		Schedulable:           acc.Schedulable,
		Groups:                groups,
		UsedPercent5h:         used5h,
		ResetAt5h:             reset5h,
		UsedPercent7d:         used7d,
		ResetAt7d:             reset7d,
		CreatedAt:             acc.CreatedAt.UTC(),
		LastErrorClass:        lastErrorClass,
		SubscriptionExpiresAt: subscriptionExpiresAt,
	}
}

func dppsExtractSeatGroups(acc *Account) []DppsSeatGroupRef {
	groups := make([]DppsSeatGroupRef, 0)
	if acc == nil {
		return groups
	}

	groupByID := make(map[int64]*Group, len(acc.Groups))
	for _, g := range acc.Groups {
		if g != nil && g.ID > 0 {
			groupByID[g.ID] = g
		}
	}

	seen := make(map[int64]struct{})
	if len(acc.AccountGroups) > 0 {
		ordered := make([]AccountGroup, len(acc.AccountGroups))
		copy(ordered, acc.AccountGroups)
		sort.SliceStable(ordered, func(i, j int) bool {
			if ordered[i].Priority != ordered[j].Priority {
				return ordered[i].Priority < ordered[j].Priority
			}
			return ordered[i].GroupID < ordered[j].GroupID
		})
		for _, ag := range ordered {
			g := ag.Group
			if g == nil {
				g = groupByID[ag.GroupID]
			}
			if g == nil || g.ID <= 0 {
				continue
			}
			if _, ok := seen[g.ID]; ok {
				continue
			}
			seen[g.ID] = struct{}{}
			groups = append(groups, DppsSeatGroupRef{ID: g.ID, Name: g.Name})
		}
		return groups
	}

	for _, g := range acc.Groups {
		if g == nil || g.ID <= 0 {
			continue
		}
		if _, ok := seen[g.ID]; ok {
			continue
		}
		seen[g.ID] = struct{}{}
		groups = append(groups, DppsSeatGroupRef{ID: g.ID, Name: g.Name})
	}
	return groups
}
