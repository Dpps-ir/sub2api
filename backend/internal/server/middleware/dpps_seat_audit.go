package middleware

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/pkg/ctxkey"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
)

const (
	// DppsSeatAuditExtraContextKey is the gin.Context key used by Seat API handlers
	// to attach non-secret summary fields (e.g. group_name, dpps_groups) to audit rows.
	DppsSeatAuditExtraContextKey = "dpps_audit_extra"

	dppsSeatAuditBodyLimit     = 64 * 1024
	dppsSeatAuditInsertTimeout = 5 * time.Second
)

var dppsSeatAuditActions = map[string]string{
	"PUT /api/v1/dpps/seats/:id/groups":               "dpps.seats.groups.update",
	"PUT /api/v1/dpps/seats/:id/scheduling-threshold": "dpps.seats.threshold.update",
	"POST /api/v1/dpps/groups":                        "dpps.groups.ensure",
	"DELETE /api/v1/dpps/groups/:id":                  "dpps.groups.delete",
	"PUT /api/v1/dpps/keys/:id/group":                 "dpps.keys.group.bind",
}

var dppsSeatAuditExtraAllowedKeys = map[string]struct{}{
	"account_id":           {},
	"group_id":             {},
	"key_id":               {},
	"group_name":           {},
	"dpps_groups":          {},
	"groups":               {},
	"groups_count":         {},
	"scheduling_threshold": {},
	"created":              {},
	"deleted":              {},
	"status":               {},
	"result":               {},
	"reason":               {},
	"error_code":           {},
}

// DppsSeatAuditMiddleware records one synchronous audit_logs row per authenticated
// mutating Seat API request (POST, PUT, PATCH, DELETE) before returning the HTTP response.
func DppsSeatAuditMiddleware(auditRepo service.AuditLogRepository) gin.HandlerFunc {
	return DppsSeatAuditMiddlewareLazy(func() service.AuditLogRepository {
		return auditRepo
	})
}

// DppsSeatAuditMiddlewareLazy resolves AuditLogRepository per request so routes
// mounted before wire finishes constructing DPPS services still audit synchronously.
func DppsSeatAuditMiddlewareLazy(resolve func() service.AuditLogRepository) gin.HandlerFunc {
	return func(c *gin.Context) {
		if c == nil || c.Request == nil || !isDppsSeatMutatingMethod(c.Request.Method) {
			if c != nil {
				c.Next()
			}
			return
		}

		bodyRedacted := captureAndRestoreDppsSeatBody(c)
		start := time.Now()
		c.Next()

		hashPrefix := c.GetString(service.DppsSeatTokenHashPrefixKey)
		if hashPrefix == "" {
			// Unauthenticated 401 requests never reach here in the route chain, and must never insert rows.
			return
		}

		maskedPrefix := hashPrefix
		if len(maskedPrefix) > 12 {
			maskedPrefix = maskedPrefix[:12]
		}

		fullPath := c.FullPath()
		action := dppsSeatAuditAction(c.Request.Method, fullPath)

		clientIP := strings.TrimSpace(c.ClientIP())
		if clientIP == "" {
			clientIP = SecurityClientIP(c)
		}

		entry := &service.AuditLog{
			CreatedAt:        time.Now().UTC(),
			ActorUserID:      nil,
			ActorEmail:       "",
			ActorRole:        service.DppsAuditActorRoleSeatAPI,
			AuthMethod:       service.DppsAuditAuthMethodSeatToken,
			CredentialMasked: "sha256:" + maskedPrefix,
			Action:           action,
			Method:           c.Request.Method,
			Path:             c.Request.URL.Path,
			RequestID:        dppsSeatAuditRequestID(c),
			ClientIP:         clientIP,
			UserAgent:        normalizePersistentText(c.Request.UserAgent(), maxPersistentUserAgentBytes),
			RequestBody:      bodyRedacted,
			StatusCode:       c.Writer.Status(),
			LatencyMs:        time.Since(start).Milliseconds(),
		}

		if extra := buildDppsSeatAuditExtra(c, fullPath); len(extra) > 0 {
			entry.Extra = extra
		}

		var repo service.AuditLogRepository
		if resolve != nil {
			repo = resolve()
		}
		if repo == nil {
			slog.Warn("dpps seat audit repository not configured, skipping audit row",
				"action", action, "method", c.Request.Method, "path", c.Request.URL.Path)
			return
		}

		insertCtx, cancel := context.WithTimeout(context.WithoutCancel(c.Request.Context()), dppsSeatAuditInsertTimeout)
		defer cancel()
		if err := repo.Insert(insertCtx, entry); err != nil {
			// Never log headers or request bodies on audit persistence failure.
			slog.Error("dpps seat audit insert failed",
				"action", action, "method", c.Request.Method, "path", c.Request.URL.Path, "error", err)
		}
	}
}

func isDppsSeatMutatingMethod(method string) bool {
	switch method {
	case http.MethodPost, http.MethodPut, http.MethodPatch, http.MethodDelete:
		return true
	default:
		return false
	}
}

func captureAndRestoreDppsSeatBody(c *gin.Context) string {
	if c.Request.Body == nil {
		return ""
	}
	orig := c.Request.Body
	raw, err := io.ReadAll(io.LimitReader(orig, dppsSeatAuditBodyLimit+1))
	if err != nil {
		c.Request.Body = &restoredBody{
			Reader: io.MultiReader(bytes.NewReader(raw), orig),
			closer: orig,
		}
		return "<unreadable body omitted>"
	}
	if len(raw) > dppsSeatAuditBodyLimit {
		c.Request.Body = &restoredBody{
			Reader: io.MultiReader(bytes.NewReader(raw), orig),
			closer: orig,
		}
		return "<body omitted: exceeds 65536 bytes>"
	}
	_ = orig.Close()
	c.Request.Body = io.NopCloser(bytes.NewReader(raw))
	return service.RedactAuditBody(raw, c.GetHeader("Content-Type"))
}

func dppsSeatAuditAction(method, fullPath string) string {
	if action, ok := dppsSeatAuditActions[method+" "+fullPath]; ok {
		return action
	}
	return "dpps.seat_api." + strings.ToLower(method)
}

func dppsSeatAuditRequestID(c *gin.Context) string {
	if c != nil && c.Request != nil {
		if rid, ok := c.Request.Context().Value(ctxkey.RequestID).(string); ok {
			if normalized, valid := normalizeCorrelationID(rid); valid {
				return normalized
			}
		}
		if rid, valid := normalizeCorrelationID(c.GetHeader(requestIDHeader)); valid {
			return rid
		}
		if rid, valid := normalizeCorrelationID(c.Writer.Header().Get(requestIDHeader)); valid {
			return rid
		}
	}
	return uuid.NewString()
}

func buildDppsSeatAuditExtra(c *gin.Context, fullPath string) map[string]any {
	extra := map[string]any{}

	if q := service.RedactAuditQuery(c.Request.URL.RawQuery); q != "" {
		extra["query"] = q
	}

	if rawID := strings.TrimSpace(c.Param("id")); rawID != "" {
		var idKey string
		switch {
		case strings.Contains(fullPath, "/seats/:id"):
			idKey = "account_id"
		case strings.Contains(fullPath, "/groups/:id"):
			idKey = "group_id"
		case strings.Contains(fullPath, "/keys/:id"):
			idKey = "key_id"
		}
		if idKey != "" {
			if parsed, err := strconv.ParseInt(rawID, 10, 64); err == nil && parsed > 0 {
				extra[idKey] = parsed
			} else {
				extra[idKey] = truncateAuditExtraString(rawID, 64)
			}
		}
	}

	if rawExtra, exists := c.Get(DppsSeatAuditExtraContextKey); exists {
		if fields, ok := rawExtra.(map[string]any); ok {
			for k, v := range fields {
				if _, allowed := dppsSeatAuditExtraAllowedKeys[k]; !allowed {
					continue
				}
				if sanitized, ok := sanitizeDppsSeatAuditExtraValue(v); ok {
					extra[k] = sanitized
				}
			}
		}
	}

	return extra
}

func sanitizeDppsSeatAuditExtraValue(value any) (any, bool) {
	if isAuditExtraScalar(value) {
		if s, ok := value.(string); ok {
			return truncateAuditExtraString(s, 256), true
		}
		return value, true
	}
	if items, ok := value.([]string); ok {
		limit := len(items)
		if limit > 200 {
			limit = 200
		}
		out := make([]string, 0, limit)
		for i := 0; i < limit; i++ {
			if trimmed := truncateAuditExtraString(items[i], 128); trimmed != "" {
				out = append(out, trimmed)
			}
		}
		return out, true
	}
	return nil, false
}
