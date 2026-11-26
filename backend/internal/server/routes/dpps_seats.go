package routes

import (
	"github.com/Wei-Shaw/sub2api/internal/handler"
	"github.com/Wei-Shaw/sub2api/internal/server/middleware"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/gin-gonic/gin"
	"github.com/redis/go-redis/v9"
)

// DppsSeatRouteDeps supplies explicit dependencies for mounting /api/v1/dpps
// in isolated tests without relying on global Wire runtime registration.
type DppsSeatRouteDeps struct {
	Handler              *handler.DppsSeatHandler
	SettingRepo          service.SettingRepository
	SettingRepoResolver  func() service.SettingRepository
	AuditLogRepo         service.AuditLogRepository
	AuditLogRepoResolver func() service.AuditLogRepository
	RedisClient          *redis.Client
	RateLimitCfg         middleware.DppsSeatRateLimitConfig
}

// RegisterDppsSeatRoutes mounts /api/v1/dpps directly on v1 using lazy
// process-wide resolvers populated when the Wire graph initializes DPPS services.
func RegisterDppsSeatRoutes(v1 *gin.RouterGroup, redisClient *redis.Client) {
	RegisterDppsSeatRoutesWithDeps(v1, DppsSeatRouteDeps{
		Handler: handler.NewDppsSeatHandlerLazy(func() handler.DppsSeatServicer {
			return service.DppsSeatInstance()
		}),
		SettingRepoResolver:  service.DppsSeatSettingRepo,
		AuditLogRepoResolver: service.DppsSeatAuditLogRepo,
		RedisClient:          redisClient,
	})
}

// RegisterDppsSeatRoutesWithDeps mounts /api/v1/dpps with the Seat API middleware chain:
// pre-auth IP rate limit -> constant-time token auth -> post-auth read/write rate limit -> synchronous write audit.
func RegisterDppsSeatRoutesWithDeps(v1 *gin.RouterGroup, deps DppsSeatRouteDeps) {
	h := deps.Handler
	if h == nil {
		h = handler.NewDppsSeatHandlerLazy(func() handler.DppsSeatServicer {
			return service.DppsSeatInstance()
		})
	}

	settingResolver := deps.SettingRepoResolver
	if settingResolver == nil {
		fixed := deps.SettingRepo
		settingResolver = func() service.SettingRepository { return fixed }
	}

	auditResolver := deps.AuditLogRepoResolver
	if auditResolver == nil {
		fixed := deps.AuditLogRepo
		auditResolver = func() service.AuditLogRepository { return fixed }
	}

	dpps := v1.Group("/dpps")
	dpps.Use(middleware.DppsSeatRateLimitPreAuth(deps.RedisClient, deps.RateLimitCfg))
	dpps.Use(middleware.DppsSeatAuthMiddlewareLazy(settingResolver))
	dpps.Use(middleware.DppsSeatRateLimitAuth(deps.RedisClient, deps.RateLimitCfg))
	dpps.Use(middleware.DppsSeatAuditMiddlewareLazy(auditResolver))
	{
		dpps.GET("/seats", h.ListSeats)
		dpps.GET("/seats/:id", h.GetSeat)
		dpps.PUT("/seats/:id/groups", h.SetSeatGroups)
		dpps.PUT("/seats/:id/scheduling-threshold", h.SetSeatSchedulingThreshold)
		dpps.POST("/groups", h.EnsureGroup)
		dpps.DELETE("/groups/:id", h.DeleteGroup)
		dpps.PUT("/keys/:id/group", h.BindKeyGroup)
		dpps.GET("/usage", h.GetUsage)
	}
}
