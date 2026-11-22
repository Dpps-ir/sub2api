package middleware

import (
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"errors"
	"net/http"
	"unicode"

	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/gin-gonic/gin"
)

// DppsSeatTokenHashPrefixKey is the gin.Context key holding the authenticated
// Seat API token's non-secret SHA-256 hex prefix.
const DppsSeatTokenHashPrefixKey = service.DppsSeatTokenHashPrefixKey

var dppsSeatDummyHashInput [32]byte

// DppsSeatAuthMiddleware enforces X-DPPS-Seat-Token authentication against the
// uncached SHA-256 digest stored in SettingRepository using constant-time comparison.
func DppsSeatAuthMiddleware(settingRepo service.SettingRepository) gin.HandlerFunc {
	return DppsSeatAuthMiddlewareLazy(func() service.SettingRepository {
		return settingRepo
	})
}

// DppsSeatAuthMiddlewareLazy resolves SettingRepository per request so routes
// mounted before wire finishes constructing DPPS services remain isolated and fail closed.
func DppsSeatAuthMiddlewareLazy(resolve func() service.SettingRepository) gin.HandlerFunc {
	return func(c *gin.Context) {
		if c == nil || c.Request == nil {
			return
		}

		headerValues := c.Request.Header.Values(service.DppsSeatTokenHeader)
		if len(headerValues) == 0 || headerValues[0] == "" {
			abortDppsSeatUnauthorized(c)
			return
		}

		raw := headerValues[0]
		validShape := len(headerValues) == 1 &&
			len(raw) <= service.DppsSeatTokenMaxLen &&
			!dppsContainsWhitespace(raw)

		var repo service.SettingRepository
		if resolve != nil {
			repo = resolve()
		}
		if repo == nil {
			AbortWithError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Internal server error")
			return
		}

		storedHex, err := repo.GetValue(c.Request.Context(), service.DppsSettingKeySeatAPITokenHash)
		if err != nil && !errors.Is(err, service.ErrSettingNotFound) {
			AbortWithError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Internal server error")
			return
		}

		storedHash, configured := service.DppsDecodeSHA256Hex(storedHex)
		if err != nil {
			configured = false
		}

		var candidateHash [32]byte
		if len(raw) <= service.DppsSeatTokenMaxLen {
			candidateHash = sha256.Sum256([]byte(raw))
		} else {
			// Hash a fixed 32-byte dummy slice on oversized inputs to avoid CPU DoS
			// while still executing a constant-time comparison.
			candidateHash = sha256.Sum256(dppsSeatDummyHashInput[:])
		}

		match := subtle.ConstantTimeCompare(candidateHash[:], storedHash[:]) == 1
		if !match || !configured || !validShape {
			abortDppsSeatUnauthorized(c)
			return
		}

		hashHex := hex.EncodeToString(candidateHash[:])
		c.Set(DppsSeatTokenHashPrefixKey, hashHex[:16])
		c.Next()
	}
}

func dppsContainsWhitespace(raw string) bool {
	for _, r := range raw {
		if unicode.IsSpace(r) {
			return true
		}
	}
	return false
}

func abortDppsSeatUnauthorized(c *gin.Context) {
	AbortWithError(c, http.StatusUnauthorized, "UNAUTHORIZED", "Unauthorized")
}
