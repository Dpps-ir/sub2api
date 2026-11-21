package routes

import (
	adminhandler "github.com/Wei-Shaw/sub2api/internal/handler/admin"
	"github.com/Wei-Shaw/sub2api/internal/server/middleware"
	"github.com/Wei-Shaw/sub2api/internal/service"

	"github.com/gin-gonic/gin"
)

// registerDppsAdminRoutes registers the DPPS admin routes under /api/v1/admin/dpps
// (see DPPS.md). The events webhook settings can send account state to an outside
// host, so a write needs step-up 2FA. The wire graph builds the router before the
// events service exists, so the routes are always registered and the handler looks
// the service up on every request (503 while there is none).
func registerDppsAdminRoutes(admin *gin.RouterGroup, stepUpAuth middleware.StepUpAuthMiddleware) {
	h := adminhandler.NewDppsEventsWebhookHandler(service.DppsEventsInstance)
	g := admin.Group("/dpps/events-webhook")
	{
		g.GET("", h.Get)
		g.PUT("", gin.HandlerFunc(stepUpAuth), h.Update)
	}
}
