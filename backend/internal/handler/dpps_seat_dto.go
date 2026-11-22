package handler

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"strconv"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/gin-gonic/gin"
)

const dppsSeatMaxBodyBytes = 64 * 1024 // 64 KiB, matching DppsSeatAuditMiddleware

// DppsIsValidGroupName reports whether name is "dpps-pool" or matches the
// canonical "dpps-plan-*" syntax in docs/contracts/seat-api.md §1.3.
func DppsIsValidGroupName(name string) bool {
	return service.DppsIsValidGroupName(name)
}

// DppsIsValidPlanGroupName reports whether name matches ^dpps-plan-[A-Za-z0-9][A-Za-z0-9_-]{0,89}$.
func DppsIsValidPlanGroupName(name string) bool {
	return service.DppsIsValidPlanGroupName(name)
}

// DppsSetSeatGroupsRequest is the strict JSON request payload for PUT /api/v1/dpps/seats/:id/groups.
type DppsSetSeatGroupsRequest struct {
	Groups []*string `json:"groups"`
}

// DppsEnsureGroupRequest is the strict JSON request payload for POST /api/v1/dpps/groups.
type DppsEnsureGroupRequest struct {
	Name *string `json:"name"`
}

// DppsBindKeyGroupRequest is the strict JSON request payload for PUT /api/v1/dpps/keys/:id/group.
type DppsBindKeyGroupRequest struct {
	GroupID *int64 `json:"group_id"`
}

// DppsSeatGroupRefDTO is a minimal active group reference attached to a seat.
type DppsSeatGroupRefDTO struct {
	ID   int64  `json:"id"`
	Name string `json:"name"`
}

// DppsSeatDTO is the strict 11-field credential-free seat representation (§2.1).
type DppsSeatDTO struct {
	ID                    int64                 `json:"id"`
	Status                string                `json:"status"`
	Schedulable           bool                  `json:"schedulable"`
	Groups                []DppsSeatGroupRefDTO `json:"groups"`
	UsedPercent5h         *float64              `json:"used_percent_5h"`
	ResetAt5h             *time.Time            `json:"reset_at_5h"`
	UsedPercent7d         *float64              `json:"used_percent_7d"`
	ResetAt7d             *time.Time            `json:"reset_at_7d"`
	CreatedAt             time.Time             `json:"created_at"`
	LastErrorClass        string                `json:"last_error_class"`
	SubscriptionExpiresAt *time.Time            `json:"subscription_expires_at,omitempty"`
}

// DppsSeatListResponseDTO is the response data for GET /api/v1/dpps/seats.
type DppsSeatListResponseDTO struct {
	Seats []DppsSeatDTO `json:"seats"`
}

// DppsSetSeatGroupsResponseDTO is the response data for PUT /api/v1/dpps/seats/:id/groups.
type DppsSetSeatGroupsResponseDTO struct {
	ID         int64                 `json:"id"`
	DppsGroups []string              `json:"dpps_groups"`
	Groups     []DppsSeatGroupRefDTO `json:"groups"`
}

// DppsEnsureGroupResponseDTO is the response data for POST /api/v1/dpps/groups.
type DppsEnsureGroupResponseDTO struct {
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

// DppsDeleteGroupResponseDTO is the response data for DELETE /api/v1/dpps/groups/:id.
type DppsDeleteGroupResponseDTO struct {
	ID      int64  `json:"id"`
	Name    string `json:"name"`
	Deleted bool   `json:"deleted"`
}

// DppsBindKeyGroupResponseDTO is the strict non-secret response data for PUT /api/v1/dpps/keys/:id/group.
type DppsBindKeyGroupResponseDTO struct {
	ID        int64     `json:"id"`
	UserID    int64     `json:"user_id"`
	GroupID   int64     `json:"group_id"`
	GroupName string    `json:"group_name"`
	Status    string    `json:"status"`
	UpdatedAt time.Time `json:"updated_at"`
}

// DppsUsageSummaryItemDTO is one aggregated (account_id, api_key_id) row from GET /api/v1/dpps/usage.
type DppsUsageSummaryItemDTO struct {
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

// DppsUsageWindowResponseDTO is the response data for GET /api/v1/dpps/usage.
type DppsUsageWindowResponseDTO struct {
	Since time.Time                 `json:"since"`
	Until time.Time                 `json:"until"`
	Items []DppsUsageSummaryItemDTO `json:"items"`
}

func dppsDecodeStrictJSON(c *gin.Context, dst any) (string, string) {
	if c == nil || c.Request == nil || c.Request.Body == nil {
		return "INVALID_JSON", "request body is required"
	}
	raw, err := io.ReadAll(io.LimitReader(c.Request.Body, dppsSeatMaxBodyBytes+1))
	if err != nil {
		return "INVALID_JSON", "failed to read request body"
	}
	if len(raw) > dppsSeatMaxBodyBytes {
		return "INVALID_JSON", "request body exceeds 64 KiB limit"
	}
	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) == 0 || trimmed[0] != '{' {
		return "INVALID_JSON", "request body must be a JSON object"
	}
	dec := json.NewDecoder(bytes.NewReader(trimmed))
	dec.DisallowUnknownFields()
	if err := dec.Decode(dst); err != nil {
		return "INVALID_JSON", "invalid JSON request body"
	}
	var trailing any
	if err := dec.Decode(&trailing); !errors.Is(err, io.EOF) {
		return "INVALID_JSON", "unexpected trailing data after JSON object"
	}
	return "", ""
}

func dppsParsePositiveInt64(raw string) (int64, bool) {
	if raw == "" {
		return 0, false
	}
	for i := 0; i < len(raw); i++ {
		if raw[i] < '0' || raw[i] > '9' {
			return 0, false
		}
	}
	val, err := strconv.ParseInt(raw, 10, 64)
	if err != nil || val <= 0 {
		return 0, false
	}
	return val, true
}

func dppsValidateAndDeduplicateGroups(raw []*string) ([]string, string, string) {
	if raw == nil {
		return nil, "INVALID_REQUEST", "groups array is required"
	}
	out := make([]string, 0, len(raw))
	seen := make(map[string]struct{}, len(raw))
	for _, elem := range raw {
		if elem == nil {
			return nil, "INVALID_REQUEST", "groups must not contain null"
		}
		name := *elem
		if !DppsIsValidGroupName(name) {
			return nil, "GROUP_PREFIX_REQUIRED", "every group must be dpps-pool or dpps-plan-*"
		}
		if _, exists := seen[name]; exists {
			continue
		}
		seen[name] = struct{}{}
		out = append(out, name)
	}
	return out, "", ""
}

func dppsParseUsageQuery(c *gin.Context) (accountID int64, since, until time.Time, errReason, errMsg string) {
	if accRaw, exists := c.GetQuery("account_id"); exists && accRaw != "" {
		parsedID, ok := dppsParsePositiveInt64(accRaw)
		if !ok {
			return 0, time.Time{}, time.Time{}, "INVALID_ACCOUNT_ID", "account_id must be a positive integer"
		}
		accountID = parsedID
	}
	sinceRaw := c.Query("since")
	untilRaw := c.Query("until")
	if sinceRaw == "" || untilRaw == "" {
		return 0, time.Time{}, time.Time{}, "INVALID_TIME_RANGE", "since and until are required RFC3339 timestamps"
	}
	parsedSince, err := time.Parse(time.RFC3339Nano, sinceRaw)
	if err != nil {
		return 0, time.Time{}, time.Time{}, "INVALID_TIME_RANGE", "invalid since timestamp"
	}
	parsedUntil, err := time.Parse(time.RFC3339Nano, untilRaw)
	if err != nil {
		return 0, time.Time{}, time.Time{}, "INVALID_TIME_RANGE", "invalid until timestamp"
	}
	if !parsedUntil.After(parsedSince) {
		return 0, time.Time{}, time.Time{}, "INVALID_TIME_RANGE", "until must be strictly after since"
	}
	if parsedUntil.Sub(parsedSince) > service.DppsUsageMaxWindow {
		return 0, time.Time{}, time.Time{}, "WINDOW_TOO_LARGE", "time range must not exceed 90 days"
	}
	return accountID, parsedSince.UTC(), parsedUntil.UTC(), "", ""
}

func dppsNormalizeUTCTimePtr(t *time.Time) *time.Time {
	if t == nil {
		return nil
	}
	u := t.UTC()
	return &u
}

func dppsGroupRefsFromService(refs []service.DppsSeatGroupRef) []DppsSeatGroupRefDTO {
	out := make([]DppsSeatGroupRefDTO, 0, len(refs))
	for _, ref := range refs {
		out = append(out, DppsSeatGroupRefDTO{
			ID:   ref.ID,
			Name: ref.Name,
		})
	}
	return out
}

func dppsSeatFromService(s service.DppsSeat) DppsSeatDTO {
	return DppsSeatDTO{
		ID:                    s.ID,
		Status:                s.Status,
		Schedulable:           s.Schedulable,
		Groups:                dppsGroupRefsFromService(s.Groups),
		UsedPercent5h:         s.UsedPercent5h,
		ResetAt5h:             dppsNormalizeUTCTimePtr(s.ResetAt5h),
		UsedPercent7d:         s.UsedPercent7d,
		ResetAt7d:             dppsNormalizeUTCTimePtr(s.ResetAt7d),
		CreatedAt:             s.CreatedAt.UTC(),
		LastErrorClass:        s.LastErrorClass,
		SubscriptionExpiresAt: dppsNormalizeUTCTimePtr(s.SubscriptionExpiresAt),
	}
}

func dppsSetSeatGroupsFromService(res *service.DppsSetSeatGroupsResult) DppsSetSeatGroupsResponseDTO {
	dppsGroups := make([]string, 0, len(res.DppsGroups))
	dppsGroups = append(dppsGroups, res.DppsGroups...)
	return DppsSetSeatGroupsResponseDTO{
		ID:         res.ID,
		DppsGroups: dppsGroups,
		Groups:     dppsGroupRefsFromService(res.Groups),
	}
}

func dppsEnsureGroupFromService(res *service.DppsEnsureGroupResult) DppsEnsureGroupResponseDTO {
	return DppsEnsureGroupResponseDTO{
		ID:                              res.ID,
		Name:                            res.Name,
		Platform:                        res.Platform,
		IsExclusive:                     res.IsExclusive,
		SubscriptionType:                res.SubscriptionType,
		AllowMessagesDispatch:           res.AllowMessagesDispatch,
		ClaudeCodeOnly:                  res.ClaudeCodeOnly,
		FallbackGroupID:                 res.FallbackGroupID,
		FallbackGroupIDOnInvalidRequest: res.FallbackGroupIDOnInvalidRequest,
		Status:                          res.Status,
		Created:                         res.Created,
	}
}

func dppsDeleteGroupFromService(res *service.DppsDeleteGroupResult) DppsDeleteGroupResponseDTO {
	return DppsDeleteGroupResponseDTO{
		ID:      res.ID,
		Name:    res.Name,
		Deleted: res.Deleted,
	}
}

func dppsBindKeyGroupFromService(res *service.DppsBindKeyGroupResult) DppsBindKeyGroupResponseDTO {
	return DppsBindKeyGroupResponseDTO{
		ID:        res.ID,
		UserID:    res.UserID,
		GroupID:   res.GroupID,
		GroupName: res.GroupName,
		Status:    res.Status,
		UpdatedAt: res.UpdatedAt.UTC(),
	}
}

func dppsUsageItemFromService(item service.DppsUsageItem) DppsUsageSummaryItemDTO {
	return DppsUsageSummaryItemDTO{
		AccountID:           item.AccountID,
		APIKeyID:            item.APIKeyID,
		Requests:            item.Requests,
		InputTokens:         item.InputTokens,
		OutputTokens:        item.OutputTokens,
		CacheCreationTokens: item.CacheCreationTokens,
		CacheReadTokens:     item.CacheReadTokens,
		TotalCost:           item.TotalCost,
		ActualCost:          item.ActualCost,
		AccountStatsCost:    item.AccountStatsCost,
	}
}
