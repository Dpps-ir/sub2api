package routes

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/Wei-Shaw/sub2api/internal/server/middleware"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
)

func dppsRoutesOf(t *testing.T) map[string]bool {
	t.Helper()
	gin.SetMode(gin.TestMode)
	r := gin.New()
	stepUp := middleware.StepUpAuthMiddleware(func(c *gin.Context) { c.AbortWithStatus(http.StatusForbidden) })
	registerDppsAdminRoutes(r.Group("/api/v1/admin"), stepUp)
	got := map[string]bool{}
	for _, ri := range r.Routes() {
		got[ri.Method+" "+ri.Path] = true
	}
	return got
}

func TestDppsAdminRoutes_EventsWebhookUnderDppsPrefixOnceTheServiceExists(t *testing.T) {
	service.SetDppsEventSink(nil)
	require.Empty(t, dppsRoutesOf(t), "no events service: nothing registered")

	service.SetDppsEventSink(&service.DppsEventsService{})
	t.Cleanup(func() { service.SetDppsEventSink(nil) })
	got := dppsRoutesOf(t)
	require.True(t, got["GET /api/v1/admin/dpps/events-webhook"])
	require.True(t, got["PUT /api/v1/admin/dpps/events-webhook"])
	require.Len(t, got, 2)
}

func TestDppsAdminRoutes_WriteNeedsStepUp(t *testing.T) {
	service.SetDppsEventSink(&service.DppsEventsService{})
	t.Cleanup(func() { service.SetDppsEventSink(nil) })
	gin.SetMode(gin.TestMode)
	r := gin.New()
	registerDppsAdminRoutes(r.Group("/api/v1/admin"), middleware.StepUpAuthMiddleware(func(c *gin.Context) { c.AbortWithStatus(http.StatusForbidden) }))

	w := httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest(http.MethodPut, "/api/v1/admin/dpps/events-webhook", nil))
	require.Equal(t, http.StatusForbidden, w.Code, "the step-up check runs before the handler")
}
