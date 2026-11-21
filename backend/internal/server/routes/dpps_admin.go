package routes

import (
	adminhandler "github.com/Wei-Shaw/sub2api/internal/handler/admin"
	"github.com/Wei-Shaw/sub2api/internal/server/middleware"
	"github.com/Wei-Shaw/sub2api/internal/service"

	"github.com/gin-gonic/gin"
)

// registerDppsAdminRoutes registers the DPPS admin routes under /api/v1/admin/dpps
// (see DPPS.md). The events webhook settings can send account state to an outside
// host, so a write needs step-up 2FA. Without the events service (unit tests that
// build the routes without the dependency graph) nothing is registered.
func registerDppsAdminRoutes(admin *gin.RouterGroup, stepUpAuth middleware.StepUpAuthMiddleware) {
	events := service.DppsEventsInstance()
	if events == nil {
		return
	}
	h := adminhandler.NewDppsEventsWebhookHandler(events)
	g := admin.Group("/dpps/events-webhook")
	{
		g.GET("", h.Get)
		g.PUT("", gin.HandlerFunc(stepUpAuth), h.Update)
	}
}
