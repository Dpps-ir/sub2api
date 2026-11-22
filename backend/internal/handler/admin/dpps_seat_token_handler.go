package admin

import (
	"net/http"

	"github.com/Wei-Shaw/sub2api/internal/pkg/response"
	"github.com/Wei-Shaw/sub2api/internal/server/middleware"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/gin-gonic/gin"
)

// DppsSeatTokenHandler manages the DPPS Seat API token lifecycle under
// /api/v1/admin/dpps/seat-token (human admin JWT only; writes require step-up 2FA).
type DppsSeatTokenHandler struct {
	tokens func() *service.DppsSeatTokenService
}

// NewDppsSeatTokenHandler constructs a Seat API token admin handler using a
// per-request service lookup (matching NewDppsEventsWebhookHandler).
func NewDppsSeatTokenHandler(tokens func() *service.DppsSeatTokenService) *DppsSeatTokenHandler {
	return &DppsSeatTokenHandler{tokens: tokens}
}

func (h *DppsSeatTokenHandler) service(c *gin.Context) *service.DppsSeatTokenService {
	if h == nil || h.tokens == nil {
		response.Error(c, http.StatusServiceUnavailable, "seat token service not available")
		return nil
	}
	svc := h.tokens()
	if svc == nil {
		response.Error(c, http.StatusServiceUnavailable, "seat token service not available")
		return nil
	}
	return svc
}

// Get returns non-secret configuration metadata for the active Seat API token.
// GET /api/v1/admin/dpps/seat-token
func (h *DppsSeatTokenHandler) Get(c *gin.Context) {
	if !requireAdminKeyJWT(c) {
		return
	}
	svc := h.service(c)
	if svc == nil {
		return
	}
	status, err := svc.GetStatus(c.Request.Context())
	if err != nil {
		response.ErrorFrom(c, err)
		return
	}
	c.Header("Cache-Control", "no-store")
	response.Success(c, status)
}

// Create generates or rotates the Seat API token, returning the plaintext secret once.
// POST /api/v1/admin/dpps/seat-token
func (h *DppsSeatTokenHandler) Create(c *gin.Context) {
	if !requireAdminKeyJWT(c) {
		return
	}
	subject, ok := middleware.GetAuthSubjectFromContext(c)
	if !ok || subject.UserID <= 0 {
		response.Error(c, http.StatusForbidden, "需要管理员身份")
		return
	}
	svc := h.service(c)
	if svc == nil {
		return
	}
	res, err := svc.Create(c.Request.Context(), subject.UserID)
	if err != nil {
		response.ErrorFrom(c, err)
		return
	}
	c.Header("Cache-Control", "no-store")
	response.Success(c, res)
}

// Delete revokes the active Seat API token immediately.
// DELETE /api/v1/admin/dpps/seat-token
func (h *DppsSeatTokenHandler) Delete(c *gin.Context) {
	if !requireAdminKeyJWT(c) {
		return
	}
	svc := h.service(c)
	if svc == nil {
		return
	}
	res, err := svc.Revoke(c.Request.Context())
	if err != nil {
		response.ErrorFrom(c, err)
		return
	}
	c.Header("Cache-Control", "no-store")
	response.Success(c, res)
}
