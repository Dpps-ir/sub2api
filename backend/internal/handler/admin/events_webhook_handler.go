package admin

import (
	"github.com/Wei-Shaw/sub2api/internal/pkg/response"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/gin-gonic/gin"
)

// EventsWebhookHandler edits the outgoing events webhook settings (SC-28).
type EventsWebhookHandler struct {
	events *service.EventsService
}

func NewEventsWebhookHandler(events *service.EventsService) *EventsWebhookHandler {
	return &EventsWebhookHandler{events: events}
}

// Get returns the settings without the secret, whether a secret is stored, and
// every event type the sender can emit.
func (h *EventsWebhookHandler) Get(c *gin.Context) {
	cfg, configured, err := h.events.Get(c.Request.Context())
	if err != nil {
		response.ErrorFrom(c, err)
		return
	}
	response.Success(c, gin.H{
		"config":            cfg,
		"secret_configured": configured,
		"all_event_types":   service.AllEventTypes,
	})
}

// Update saves the settings. An empty secret keeps the stored one.
func (h *EventsWebhookHandler) Update(c *gin.Context) {
	var req service.EventsWebhookConfig
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
