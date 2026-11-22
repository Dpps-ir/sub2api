package service

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"time"
)

const (
	// DppsSeatTokenHeader is the sole HTTP header accepted for DPPS Seat API requests.
	DppsSeatTokenHeader = "X-DPPS-Seat-Token"
	// DppsSeatTokenPrefix is the fixed prefix of generated DPPS Seat API tokens.
	DppsSeatTokenPrefix = "dpps_seat_"
	// DppsSeatTokenRandomBytes is the entropy size (256-bit) of generated Seat API tokens.
	DppsSeatTokenRandomBytes = 32
	// DppsSeatTokenMaxLen caps incoming X-DPPS-Seat-Token length before hashing.
	DppsSeatTokenMaxLen = 256

	// DppsSettingKeySeatAPITokenHash stores only the lowercase SHA-256 hex digest of the active token.
	DppsSettingKeySeatAPITokenHash = "dpps_seat_api_token_hash"
	// DppsSettingKeySeatAPITokenMeta stores non-secret metadata (4-hex prefix, updated_at, updated_by).
	DppsSettingKeySeatAPITokenMeta = "dpps_seat_api_token_meta"

	// DppsAuditAuthMethodSeatToken and DppsAuditActorRoleSeatAPI identify Seat API audit log entries.
	DppsAuditAuthMethodSeatToken = "dpps_seat_token"
	DppsAuditActorRoleSeatAPI    = "dpps_seat_api"

	// DppsSeatTokenHashPrefixKey stores the non-secret SHA-256 hex prefix in gin.Context after auth.
	DppsSeatTokenHashPrefixKey = "dpps_seat_token_hash_prefix"
)

// DppsSeatTokenStatus describes whether a Seat API token is configured without
// exposing the plaintext token or its SHA-256 hash.
type DppsSeatTokenStatus struct {
	Configured  bool   `json:"configured"`
	TokenPrefix string `json:"token_prefix"`
	UpdatedAt   string `json:"updated_at"`
}

// DppsSeatTokenRotateResult is the one-time reveal response returned on token generation/rotation.
type DppsSeatTokenRotateResult struct {
	Token       string `json:"token"`
	TokenPrefix string `json:"token_prefix"`
	UpdatedAt   string `json:"updated_at"`
}

// DppsSeatTokenDeleteResult is returned after revoking the Seat API token.
type DppsSeatTokenDeleteResult struct {
	Configured bool `json:"configured"`
	Deleted    bool `json:"deleted"`
}

type dppsSeatTokenMeta struct {
	Prefix    string `json:"prefix"`
	UpdatedAt string `json:"updated_at"`
	UpdatedBy int64  `json:"updated_by"`
}

// DppsSeatTokenService manages provisioning, rotation, and revocation of the
// service-to-service Seat API token using SHA-256 hash-only storage.
type DppsSeatTokenService struct {
	settingRepo SettingRepository
	now         func() time.Time
}

// NewDppsSeatTokenService creates a Seat API token service backed by SettingRepository.
func NewDppsSeatTokenService(settingRepo SettingRepository) *DppsSeatTokenService {
	return &DppsSeatTokenService{
		settingRepo: settingRepo,
		now:         time.Now,
	}
}

// GetStatus returns non-secret configuration metadata for the active Seat API token.
func (s *DppsSeatTokenService) GetStatus(ctx context.Context) (*DppsSeatTokenStatus, error) {
	if s == nil || s.settingRepo == nil {
		return nil, errors.New("seat token repository not configured")
	}

	hashHex, err := s.settingRepo.GetValue(ctx, DppsSettingKeySeatAPITokenHash)
	if err != nil {
		if errors.Is(err, ErrSettingNotFound) {
			return &DppsSeatTokenStatus{Configured: false, TokenPrefix: "", UpdatedAt: ""}, nil
		}
		return nil, err
	}
	if _, ok := DppsDecodeSHA256Hex(hashHex); !ok {
		return &DppsSeatTokenStatus{Configured: false, TokenPrefix: "", UpdatedAt: ""}, nil
	}

	metaRaw, err := s.settingRepo.GetValue(ctx, DppsSettingKeySeatAPITokenMeta)
	if err != nil {
		if errors.Is(err, ErrSettingNotFound) {
			return &DppsSeatTokenStatus{Configured: true, TokenPrefix: "", UpdatedAt: ""}, nil
		}
		return nil, err
	}

	var meta dppsSeatTokenMeta
	if err := json.Unmarshal([]byte(metaRaw), &meta); err != nil {
		return &DppsSeatTokenStatus{Configured: true, TokenPrefix: "", UpdatedAt: ""}, nil
	}

	return &DppsSeatTokenStatus{
		Configured:  true,
		TokenPrefix: sanitizeDppsSeatTokenPrefix(meta.Prefix),
		UpdatedAt:   strings.TrimSpace(meta.UpdatedAt),
	}, nil
}

// Create generates a new 256-bit Seat API token, stores only its SHA-256 digest
// and non-secret metadata, and returns the plaintext token once.
func (s *DppsSeatTokenService) Create(ctx context.Context, adminUserID int64) (*DppsSeatTokenRotateResult, error) {
	if s == nil || s.settingRepo == nil {
		return nil, errors.New("seat token repository not configured")
	}

	buf := make([]byte, DppsSeatTokenRandomBytes)
	if _, err := rand.Read(buf); err != nil {
		return nil, fmt.Errorf("generate seat token: %w", err)
	}
	hexBody := hex.EncodeToString(buf)
	rawToken := DppsSeatTokenPrefix + hexBody

	sum := sha256.Sum256([]byte(rawToken))
	hashHex := hex.EncodeToString(sum[:])

	nowFn := s.now
	if nowFn == nil {
		nowFn = time.Now
	}
	updatedAt := nowFn().UTC().Format(time.RFC3339)
	prefix := DppsSeatTokenPrefix + hexBody[:4]

	metaBytes, err := json.Marshal(dppsSeatTokenMeta{
		Prefix:    prefix,
		UpdatedAt: updatedAt,
		UpdatedBy: adminUserID,
	})
	if err != nil {
		return nil, fmt.Errorf("marshal seat token metadata: %w", err)
	}

	// Write metadata first so the active hash never points to an orphaned token if meta fails.
	if err := s.settingRepo.Set(ctx, DppsSettingKeySeatAPITokenMeta, string(metaBytes)); err != nil {
		return nil, err
	}
	if err := s.settingRepo.Set(ctx, DppsSettingKeySeatAPITokenHash, hashHex); err != nil {
		return nil, err
	}

	return &DppsSeatTokenRotateResult{
		Token:       rawToken,
		TokenPrefix: prefix,
		UpdatedAt:   updatedAt,
	}, nil
}

// Revoke deletes the stored Seat API token hash and metadata, immediately
// invalidating all subsequent Seat API requests.
func (s *DppsSeatTokenService) Revoke(ctx context.Context) (*DppsSeatTokenDeleteResult, error) {
	if s == nil || s.settingRepo == nil {
		return nil, errors.New("seat token repository not configured")
	}

	// Delete the hash first so revocation takes effect immediately even if metadata cleanup fails.
	if err := s.settingRepo.Delete(ctx, DppsSettingKeySeatAPITokenHash); err != nil && !errors.Is(err, ErrSettingNotFound) {
		return nil, err
	}
	if err := s.settingRepo.Delete(ctx, DppsSettingKeySeatAPITokenMeta); err != nil && !errors.Is(err, ErrSettingNotFound) {
		return nil, err
	}

	return &DppsSeatTokenDeleteResult{
		Configured: false,
		Deleted:    true,
	}, nil
}

// DppsDecodeSHA256Hex decodes a 64-character hex SHA-256 digest into a 32-byte array.
func DppsDecodeSHA256Hex(raw string) ([32]byte, bool) {
	var out [32]byte
	if len(raw) != 64 {
		return out, false
	}
	decoded, err := hex.DecodeString(raw)
	if err != nil || len(decoded) != 32 {
		return out, false
	}
	copy(out[:], decoded)
	return out, true
}

func sanitizeDppsSeatTokenPrefix(prefix string) string {
	prefix = strings.TrimSpace(prefix)
	maxLen := len(DppsSeatTokenPrefix) + 4
	if !strings.HasPrefix(prefix, DppsSeatTokenPrefix) || len(prefix) > maxLen {
		return ""
	}
	return prefix
}

type dppsSeatTokenInstanceHolder struct {
	svc *DppsSeatTokenService
}

var dppsGlobalSeatTokenInstance atomic.Pointer[dppsSeatTokenInstanceHolder]

// SetDppsSeatTokenInstance installs the process-wide Seat API token service.
// Passing nil clears it (for tests).
func SetDppsSeatTokenInstance(svc *DppsSeatTokenService) {
	if svc == nil {
		dppsGlobalSeatTokenInstance.Store(nil)
		return
	}
	dppsGlobalSeatTokenInstance.Store(&dppsSeatTokenInstanceHolder{svc: svc})
}

// DppsSeatTokenInstance returns the installed Seat API token service, or nil.
func DppsSeatTokenInstance() *DppsSeatTokenService {
	if h := dppsGlobalSeatTokenInstance.Load(); h != nil {
		return h.svc
	}
	return nil
}

type dppsSeatMiddlewareReposHolder struct {
	settingRepo SettingRepository
	auditRepo   AuditLogRepository
}

var dppsGlobalSeatMiddlewareRepos atomic.Pointer[dppsSeatMiddlewareReposHolder]

// SetDppsSeatMiddlewareRepos installs the repositories used by lazy Seat API auth
// and audit middlewares wired before the DPPS provider graph finishes building.
func SetDppsSeatMiddlewareRepos(settingRepo SettingRepository, auditRepo AuditLogRepository) {
	if settingRepo == nil && auditRepo == nil {
		dppsGlobalSeatMiddlewareRepos.Store(nil)
		return
	}
	dppsGlobalSeatMiddlewareRepos.Store(&dppsSeatMiddlewareReposHolder{
		settingRepo: settingRepo,
		auditRepo:   auditRepo,
	})
}

// DppsSeatSettingRepo returns the installed SettingRepository for Seat API auth, or nil.
func DppsSeatSettingRepo() SettingRepository {
	if h := dppsGlobalSeatMiddlewareRepos.Load(); h != nil {
		return h.settingRepo
	}
	return nil
}

// DppsSeatAuditLogRepo returns the installed AuditLogRepository for Seat API audit, or nil.
func DppsSeatAuditLogRepo() AuditLogRepository {
	if h := dppsGlobalSeatMiddlewareRepos.Load(); h != nil {
		return h.auditRepo
	}
	return nil
}
