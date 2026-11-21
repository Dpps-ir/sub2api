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
	require.Len(t, dppsRoutesOf(t), 2, "routes exist even before the service does")

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

// The real wire graph builds the router before the events service (wire_gen.go:
// server.ProvideRouter, then dpps.ProvideEventsService), so the routes must exist
// when the service is still nil and start working once it is installed.
func TestDppsAdminRoutes_RegisteredBeforeTheServiceExists(t *testing.T) {
	service.SetDppsEventSink(nil)
	t.Cleanup(func() { service.SetDppsEventSink(nil) })
	gin.SetMode(gin.TestMode)
	r := gin.New()
	registerDppsAdminRoutes(r.Group("/api/v1/admin"), middleware.StepUpAuthMiddleware(func(c *gin.Context) { c.AbortWithStatus(http.StatusForbidden) }))

	for _, m := range []string{http.MethodGet, http.MethodPut} {
		w := httptest.NewRecorder()
		r.ServeHTTP(w, httptest.NewRequest(m, "/api/v1/admin/dpps/events-webhook", nil))
		require.NotEqual(t, http.StatusNotFound, w.Code, m+": the route must exist before the service does")
	}
	w := httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/api/v1/admin/dpps/events-webhook", nil))
	require.Equal(t, http.StatusServiceUnavailable, w.Code, "no service yet: 503")
	require.Contains(t, w.Body.String(), "events webhook not available")
}
