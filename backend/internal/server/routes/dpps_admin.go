package routes

import (
	adminhandler "github.com/Wei-Shaw/sub2api/internal/handler/admin"
	"github.com/Wei-Shaw/sub2api/internal/server/middleware"
	"github.com/Wei-Shaw/sub2api/internal/service"

	"github.com/gin-gonic/gin"
)

// registerDppsAdminRoutes registers the DPPS admin routes under /api/v1/admin/dpps
// (see DPPS.md). The events webhook settings can send account state to an outside
// host and the seat-token routes mint service credentials, so writes need step-up 2FA.
// The wire graph builds the router before DPPS services exist, so routes are always
// registered and handlers resolve each service per request (503 while absent).
func registerDppsAdminRoutes(admin *gin.RouterGroup, stepUpAuth middleware.StepUpAuthMiddleware) {
	h := adminhandler.NewDppsEventsWebhookHandler(service.DppsEventsInstance)
	g := admin.Group("/dpps/events-webhook")
	{
		g.GET("", h.Get)
		g.PUT("", gin.HandlerFunc(stepUpAuth), h.Update)
	}

	th := adminhandler.NewDppsSeatTokenHandler(service.DppsSeatTokenInstance)
	tg := admin.Group("/dpps/seat-token")
	{
		tg.GET("", th.Get)
		tg.POST("", gin.HandlerFunc(stepUpAuth), th.Create)
		tg.DELETE("", gin.HandlerFunc(stepUpAuth), th.Delete)
	}
}
