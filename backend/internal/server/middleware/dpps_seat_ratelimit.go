package middleware

import (
	"context"
	"log/slog"
	"net/http"
	"net/netip"
	"strconv"
	"strings"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/middleware"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/gin-gonic/gin"
	"github.com/redis/go-redis/v9"
)

const (
	// DppsSeatDefaultPreAuthPerMin is the default pre-auth IP quota per minute.
	DppsSeatDefaultPreAuthPerMin = 600
	// DppsSeatDefaultReadPerMin is the default post-auth read quota per minute.
	DppsSeatDefaultReadPerMin = 600
	// DppsSeatDefaultWritePerMin is the default post-auth write quota per minute.
	DppsSeatDefaultWritePerMin = 120

	dppsSeatRateLimitDefaultWindow = time.Minute
)

// DppsSeatRateLimitConfig configures fixed-window Seat API rate limits.
// Zero or negative values fall back to the contract defaults.
type DppsSeatRateLimitConfig struct {
	PreAuthPerMin int
	ReadPerMin    int
	WritePerMin   int
	Window        time.Duration
}

type dppsSeatRateLimitAllower interface {
	Allow(ctx context.Context, key string, limit int, window time.Duration) (middleware.AllowResult, error)
}

func normalizeDppsSeatRateLimitConfig(cfg DppsSeatRateLimitConfig) DppsSeatRateLimitConfig {
	if cfg.PreAuthPerMin <= 0 {
		cfg.PreAuthPerMin = DppsSeatDefaultPreAuthPerMin
	}
	if cfg.ReadPerMin <= 0 {
		cfg.ReadPerMin = DppsSeatDefaultReadPerMin
	}
	if cfg.WritePerMin <= 0 {
		cfg.WritePerMin = DppsSeatDefaultWritePerMin
	}
	if cfg.Window <= 0 {
		cfg.Window = dppsSeatRateLimitDefaultWindow
	}
	return cfg
}

func newDppsSeatRateAllower(redisClient *redis.Client) dppsSeatRateLimitAllower {
	if redisClient == nil {
		return nil
	}
	return middleware.NewRateLimiter(redisClient)
}

// DppsSeatRateLimitPreAuth enforces the pre-auth per-client-IP rate limit on
// /api/v1/dpps/*, including loopback and private-network callers.
func DppsSeatRateLimitPreAuth(redisClient *redis.Client, cfg DppsSeatRateLimitConfig) gin.HandlerFunc {
	return dppsSeatRateLimitPreAuthWithAllower(newDppsSeatRateAllower(redisClient), cfg)
}

func dppsSeatRateLimitPreAuthWithAllower(allower dppsSeatRateLimitAllower, cfg DppsSeatRateLimitConfig) gin.HandlerFunc {
	cfg = normalizeDppsSeatRateLimitConfig(cfg)
	return func(c *gin.Context) {
		if allower == nil {
			slog.Warn("dpps seat pre-auth rate limiter disabled (nil redis client), allowing request", "scope", "ip")
			c.Next()
			return
		}
		key := "dpps_seat:ip:" + dppsSeatClientIPBucket(c)
		res, err := allower.Allow(c.Request.Context(), key, cfg.PreAuthPerMin, cfg.Window)
		if err != nil {
			slog.Warn("dpps seat rate limit check failed, allowing request", "scope", "ip", "error", err)
			c.Next()
			return
		}
		if !res.Allowed {
			abortDppsSeatRateLimited(c, res.RetryAfter, cfg.Window)
			return
		}
		c.Next()
	}
}

// DppsSeatRateLimitAuth enforces post-auth per-token read and write rate limits
// keyed by the authenticated token's 16-hex SHA-256 prefix.
func DppsSeatRateLimitAuth(redisClient *redis.Client, cfg DppsSeatRateLimitConfig) gin.HandlerFunc {
	return dppsSeatRateLimitAuthWithAllower(newDppsSeatRateAllower(redisClient), cfg)
}

func dppsSeatRateLimitAuthWithAllower(allower dppsSeatRateLimitAllower, cfg DppsSeatRateLimitConfig) gin.HandlerFunc {
	cfg = normalizeDppsSeatRateLimitConfig(cfg)
	return func(c *gin.Context) {
		hashPrefix := c.GetString(service.DppsSeatTokenHashPrefixKey)
		if hashPrefix == "" {
			abortDppsSeatUnauthorized(c)
			return
		}
		if len(hashPrefix) > 16 {
			hashPrefix = hashPrefix[:16]
		}
		if allower == nil {
			slog.Warn("dpps seat auth rate limiter disabled (nil redis client), allowing request", "scope", "auth")
			c.Next()
			return
		}

		scope := "write"
		limit := cfg.WritePerMin
		if c.Request != nil && (c.Request.Method == http.MethodGet || c.Request.Method == http.MethodHead) {
			scope = "read"
			limit = cfg.ReadPerMin
		}

		key := "dpps_seat:" + scope + ":" + hashPrefix
		res, err := allower.Allow(c.Request.Context(), key, limit, cfg.Window)
		if err != nil {
			slog.Warn("dpps seat rate limit check failed, allowing request", "scope", scope, "error", err)
			c.Next()
			return
		}
		if !res.Allowed {
			abortDppsSeatRateLimited(c, res.RetryAfter, cfg.Window)
			return
		}
		c.Next()
	}
}

func dppsSeatClientIPBucket(c *gin.Context) string {
	clientIP := strings.TrimSpace(SecurityClientIP(c))
	if clientIP == "" && c != nil {
		clientIP = strings.TrimSpace(c.ClientIP())
	}
	if clientIP == "" {
		return "unknown"
	}
	addr, err := netip.ParseAddr(clientIP)
	if err != nil {
		return clientIP
	}
	if addr.Is4In6() {
		return addr.Unmap().String()
	}
	if !addr.Is6() {
		return clientIP
	}
	prefix, err := addr.WithZone("").Prefix(64)
	if err != nil {
		return clientIP
	}
	return prefix.String()
}

func abortDppsSeatRateLimited(c *gin.Context, retryAfter, window time.Duration) {
	if retryAfter <= 0 {
		retryAfter = window
	}
	seconds := int64(retryAfter / time.Second)
	if retryAfter%time.Second > 0 {
		seconds++
	}
	if seconds < 1 {
		seconds = 1
	}
	c.Header("Retry-After", strconv.FormatInt(seconds, 10))
	AbortWithError(c, http.StatusTooManyRequests, "RATE_LIMITED", "Too many requests, please slow down and try again later")
}
