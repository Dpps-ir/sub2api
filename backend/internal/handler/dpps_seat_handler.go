package handler

import (
	"context"
	"errors"
	"net/http"
	"reflect"
	"time"

	infraerrors "github.com/Wei-Shaw/sub2api/internal/pkg/errors"
	"github.com/Wei-Shaw/sub2api/internal/pkg/response"
	"github.com/Wei-Shaw/sub2api/internal/server/middleware"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/gin-gonic/gin"
)

// DppsSeatServicer defines the Seat API operations required by DppsSeatHandler.
type DppsSeatServicer interface {
	ListSeats(ctx context.Context) ([]service.DppsSeat, error)
	GetSeat(ctx context.Context, accountID int64) (*service.DppsSeat, error)
	SetSeatGroups(ctx context.Context, accountID int64, groupNames []string) (*service.DppsSetSeatGroupsResult, error)
	EnsureGroup(ctx context.Context, name string) (*service.DppsEnsureGroupResult, error)
	DeleteEmptyPlanGroup(ctx context.Context, groupID int64) (*service.DppsDeleteGroupResult, error)
	BindKeyGroup(ctx context.Context, keyID, groupID int64) (*service.DppsBindKeyGroupResult, error)
	GetUsageWindow(ctx context.Context, accountID int64, since, until time.Time) ([]service.DppsUsageItem, error)
}

var _ DppsSeatServicer = (*service.DppsSeatService)(nil)

// DppsSeatHandler exposes the DPPS Seat API HTTP endpoints under /api/v1/dpps.
type DppsSeatHandler struct {
	resolve func() DppsSeatServicer
}

// NewDppsSeatHandler creates a DppsSeatHandler bound to a fixed DppsSeatServicer.
func NewDppsSeatHandler(svc DppsSeatServicer) *DppsSeatHandler {
	return &DppsSeatHandler{
		resolve: func() DppsSeatServicer {
			return svc
		},
	}
}

// NewDppsSeatHandlerLazy resolves the Seat API service per request so routes mounted
// before Wire builds DPPS services can look up DppsSeatInstance safely.
func NewDppsSeatHandlerLazy(resolve func() DppsSeatServicer) *DppsSeatHandler {
	return &DppsSeatHandler{resolve: resolve}
}

// ListSeats handles GET /api/v1/dpps/seats.
func (h *DppsSeatHandler) ListSeats(c *gin.Context) {
	svc := h.service(c)
	if svc == nil {
		return
	}
	seats, err := svc.ListSeats(c.Request.Context())
	if err != nil {
		dppsHandleServiceError(c, err)
		return
	}
	out := make([]DppsSeatDTO, 0, len(seats))
	for _, seat := range seats {
		out = append(out, dppsSeatFromService(seat))
	}
	response.Success(c, DppsSeatListResponseDTO{Seats: out})
}

// GetSeat handles GET /api/v1/dpps/seats/:id.
func (h *DppsSeatHandler) GetSeat(c *gin.Context) {
	accountID, ok := dppsParsePositiveInt64(c.Param("id"))
	if !ok {
		dppsWriteError(c, http.StatusBadRequest, "invalid account id", "INVALID_ACCOUNT_ID")
		return
	}
	svc := h.service(c)
	if svc == nil {
		return
	}
	seat, err := svc.GetSeat(c.Request.Context(), accountID)
	if err != nil {
		dppsHandleServiceError(c, err)
		return
	}
	if seat == nil {
		dppsWriteError(c, http.StatusNotFound, "seat not found", "SEAT_NOT_FOUND")
		return
	}
	response.Success(c, dppsSeatFromService(*seat))
}

// SetSeatGroups handles PUT /api/v1/dpps/seats/:id/groups.
func (h *DppsSeatHandler) SetSeatGroups(c *gin.Context) {
	accountID, ok := dppsParsePositiveInt64(c.Param("id"))
	if !ok {
		dppsWriteError(c, http.StatusBadRequest, "invalid account id", "INVALID_ACCOUNT_ID")
		return
	}

	var req DppsSetSeatGroupsRequest
	if reason, msg := dppsDecodeStrictJSON(c, &req); reason != "" {
		dppsWriteError(c, http.StatusBadRequest, msg, reason)
		return
	}
	groupNames, reason, msg := dppsValidateAndDeduplicateGroups(req.Groups)
	if reason != "" {
		dppsWriteError(c, http.StatusBadRequest, msg, reason)
		return
	}

	svc := h.service(c)
	if svc == nil {
		return
	}
	res, err := svc.SetSeatGroups(c.Request.Context(), accountID, groupNames)
	if err != nil {
		dppsHandleServiceError(c, err)
		return
	}
	if res == nil {
		dppsWriteError(c, http.StatusInternalServerError, "internal server error", "INTERNAL_ERROR")
		return
	}

	out := dppsSetSeatGroupsFromService(res)
	dppsSetAuditExtra(c, map[string]any{
		"account_id":   out.ID,
		"dpps_groups":  out.DppsGroups,
		"groups_count": len(out.Groups),
		"result":       "ok",
	})
	response.Success(c, out)
}

// EnsureGroup handles POST /api/v1/dpps/groups.
func (h *DppsSeatHandler) EnsureGroup(c *gin.Context) {
	var req DppsEnsureGroupRequest
	if reason, msg := dppsDecodeStrictJSON(c, &req); reason != "" {
		dppsWriteError(c, http.StatusBadRequest, msg, reason)
		return
	}
	if req.Name == nil || !DppsIsValidGroupName(*req.Name) {
		dppsWriteError(c, http.StatusBadRequest, "invalid group name", "INVALID_GROUP_NAME")
		return
	}

	svc := h.service(c)
	if svc == nil {
		return
	}
	res, err := svc.EnsureGroup(c.Request.Context(), *req.Name)
	if err != nil {
		dppsHandleServiceError(c, err)
		return
	}
	if res == nil {
		dppsWriteError(c, http.StatusInternalServerError, "internal server error", "INTERNAL_ERROR")
		return
	}

	out := dppsEnsureGroupFromService(res)
	dppsSetAuditExtra(c, map[string]any{
		"group_id":   out.ID,
		"group_name": out.Name,
		"created":    out.Created,
		"result":     "ok",
	})
	response.Success(c, out)
}

// DeleteGroup handles DELETE /api/v1/dpps/groups/:id.
func (h *DppsSeatHandler) DeleteGroup(c *gin.Context) {
	groupID, ok := dppsParsePositiveInt64(c.Param("id"))
	if !ok {
		dppsWriteError(c, http.StatusBadRequest, "invalid group id", "INVALID_GROUP_ID")
		return
	}

	svc := h.service(c)
	if svc == nil {
		return
	}
	res, err := svc.DeleteEmptyPlanGroup(c.Request.Context(), groupID)
	if err != nil {
		dppsHandleServiceError(c, err)
		return
	}
	if res == nil {
		dppsWriteError(c, http.StatusInternalServerError, "internal server error", "INTERNAL_ERROR")
		return
	}

	out := dppsDeleteGroupFromService(res)
	dppsSetAuditExtra(c, map[string]any{
		"group_id":   out.ID,
		"group_name": out.Name,
		"deleted":    out.Deleted,
		"result":     "ok",
	})
	response.Success(c, out)
}

// BindKeyGroup handles PUT /api/v1/dpps/keys/:id/group.
func (h *DppsSeatHandler) BindKeyGroup(c *gin.Context) {
	keyID, ok := dppsParsePositiveInt64(c.Param("id"))
	if !ok {
		dppsWriteError(c, http.StatusBadRequest, "invalid key id", "INVALID_KEY_ID")
		return
	}

	var req DppsBindKeyGroupRequest
	if reason, msg := dppsDecodeStrictJSON(c, &req); reason != "" {
		dppsWriteError(c, http.StatusBadRequest, msg, reason)
		return
	}
	if req.GroupID == nil || *req.GroupID <= 0 {
		dppsWriteError(c, http.StatusBadRequest, "group_id must be a positive integer", "INVALID_GROUP_ID")
		return
	}

	svc := h.service(c)
	if svc == nil {
		return
	}
	res, err := svc.BindKeyGroup(c.Request.Context(), keyID, *req.GroupID)
	if err != nil {
		dppsHandleServiceError(c, err)
		return
	}
	if res == nil {
		dppsWriteError(c, http.StatusInternalServerError, "internal server error", "INTERNAL_ERROR")
		return
	}

	out := dppsBindKeyGroupFromService(res)
	dppsSetAuditExtra(c, map[string]any{
		"key_id":     out.ID,
		"group_id":   out.GroupID,
		"group_name": out.GroupName,
		"status":     out.Status,
		"result":     "ok",
	})
	response.Success(c, out)
}

// GetUsage handles GET /api/v1/dpps/usage.
func (h *DppsSeatHandler) GetUsage(c *gin.Context) {
	accountID, since, until, reason, msg := dppsParseUsageQuery(c)
	if reason != "" {
		dppsWriteError(c, http.StatusBadRequest, msg, reason)
		return
	}

	svc := h.service(c)
	if svc == nil {
		return
	}
	items, err := svc.GetUsageWindow(c.Request.Context(), accountID, since, until)
	if err != nil {
		dppsHandleServiceError(c, err)
		return
	}

	out := make([]DppsUsageSummaryItemDTO, 0, len(items))
	for _, item := range items {
		out = append(out, dppsUsageItemFromService(item))
	}
	response.Success(c, DppsUsageWindowResponseDTO{
		Since: since,
		Until: until,
		Items: out,
	})
}

func (h *DppsSeatHandler) service(c *gin.Context) DppsSeatServicer {
	if h == nil || h.resolve == nil {
		dppsWriteError(c, http.StatusServiceUnavailable, "seat service unavailable", "SERVICE_UNAVAILABLE")
		return nil
	}
	svc := h.resolve()
	if dppsIsNilServicer(svc) {
		dppsWriteError(c, http.StatusServiceUnavailable, "seat service unavailable", "SERVICE_UNAVAILABLE")
		return nil
	}
	return svc
}

func dppsIsNilServicer(svc DppsSeatServicer) bool {
	if svc == nil {
		return true
	}
	v := reflect.ValueOf(svc)
	return v.Kind() == reflect.Ptr && v.IsNil()
}

func dppsSetAuditExtra(c *gin.Context, fields map[string]any) {
	if c == nil || len(fields) == 0 {
		return
	}
	merged := map[string]any{}
	if existingRaw, exists := c.Get(middleware.DppsSeatAuditExtraContextKey); exists {
		if existing, ok := existingRaw.(map[string]any); ok {
			for k, v := range existing {
				merged[k] = v
			}
		}
	}
	for k, v := range fields {
		merged[k] = v
	}
	c.Set(middleware.DppsSeatAuditExtraContextKey, merged)
}

func dppsWriteError(c *gin.Context, statusCode int, message, reason string) {
	if reason != "" {
		dppsSetAuditExtra(c, map[string]any{
			"result":     "error",
			"reason":     reason,
			"error_code": reason,
		})
	}
	response.ErrorWithDetails(c, statusCode, message, reason, nil)
}

func dppsHandleServiceError(c *gin.Context, err error) {
	if errors.Is(err, service.ErrAccountNotFound) {
		dppsWriteError(c, http.StatusNotFound, "seat not found", "SEAT_NOT_FOUND")
		return
	}
	var appErr *infraerrors.ApplicationError
	if errors.As(err, &appErr) {
		if appErr.Reason == "ACCOUNT_NOT_FOUND" {
			dppsWriteError(c, http.StatusNotFound, "seat not found", "SEAT_NOT_FOUND")
			return
		}
		if appErr.Code >= http.StatusInternalServerError {
			dppsSetAuditExtra(c, map[string]any{
				"result":     "error",
				"error_code": "INTERNAL_ERROR",
			})
			response.ErrorFrom(c, err)
			return
		}
		dppsWriteError(c, int(appErr.Code), appErr.Message, appErr.Reason)
		return
	}
	dppsSetAuditExtra(c, map[string]any{
		"result":     "error",
		"error_code": "INTERNAL_ERROR",
	})
	response.ErrorFrom(c, err)
}
