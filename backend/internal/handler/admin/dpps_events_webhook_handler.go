package admin

import (
	"github.com/Wei-Shaw/sub2api/internal/pkg/response"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/gin-gonic/gin"
)

// DppsEventsWebhookHandler edits the outgoing events webhook settings (SC-28).
type DppsEventsWebhookHandler struct {
	events *service.DppsEventsService
}

func NewDppsEventsWebhookHandler(events *service.DppsEventsService) *DppsEventsWebhookHandler {
	return &DppsEventsWebhookHandler{events: events}
}

// Get returns the settings without the secret, whether a secret is stored, and
// every event type the sender can emit.
func (h *DppsEventsWebhookHandler) Get(c *gin.Context) {
	cfg, configured, err := h.events.Get(c.Request.Context())
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
	var req service.DppsEventsWebhookConfig
	if err := c.ShouldBindJSON(&req); err != nil {
		response.BadRequest(c, "Invalid request: "+err.Error())
		return
	}
	cfg, err := h.events.Update(c.Request.Context(), req)
	if err != nil {
		response.ErrorFrom(c, err)
		return
	}
	response.Success(c, cfg)
}
