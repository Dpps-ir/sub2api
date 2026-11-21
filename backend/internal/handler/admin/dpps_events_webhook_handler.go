package admin

import (
	"net/http"

	"github.com/Wei-Shaw/sub2api/internal/pkg/response"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/gin-gonic/gin"
)

// DppsEventsWebhookHandler edits the outgoing events webhook settings (SC-28).
type DppsEventsWebhookHandler struct {
	events func() *service.DppsEventsService
}

// NewDppsEventsWebhookHandler takes a lookup, not the service: the router is built
// before the service in the wire graph, so the service is resolved per request.
func NewDppsEventsWebhookHandler(events func() *service.DppsEventsService) *DppsEventsWebhookHandler {
	return &DppsEventsWebhookHandler{events: events}
}

func (h *DppsEventsWebhookHandler) service(c *gin.Context) *service.DppsEventsService {
	svc := h.events()
	if svc == nil {
		response.Error(c, http.StatusServiceUnavailable, "events webhook not available")
	}
	return svc
}

// Get returns the settings without the secret, whether a secret is stored, and
// every event type the sender can emit.
func (h *DppsEventsWebhookHandler) Get(c *gin.Context) {
	svc := h.service(c)
	if svc == nil {
		return
	}
	cfg, configured, err := svc.Get(c.Request.Context())
	if err != nil {
		response.ErrorFrom(c, err)
		return
	}
	response.Success(c, gin.H{
		"config":            cfg,
		"secret_configured": configured,
		"all_event_types":   service.DppsAllEventTypes,
	})
}

// Update saves the settings. An empty secret keeps the stored one.
func (h *DppsEventsWebhookHandler) Update(c *gin.Context) {
	svc := h.service(c)
	if svc == nil {
		return
	}
	var req service.DppsEventsWebhookConfig
	if err := c.ShouldBindJSON(&req); err != nil {
		response.BadRequest(c, "Invalid request: "+err.Error())
		return
	}
	cfg, err := svc.Update(c.Request.Context(), req)
	if err != nil {
		response.ErrorFrom(c, err)
		return
	}
	response.Success(c, cfg)
}
