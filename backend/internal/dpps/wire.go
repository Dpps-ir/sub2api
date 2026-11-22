// Package dpps holds the dependency-injection wiring of the DPPS additions to
// Sub2API (see DPPS.md at the repository root). cmd/server/wire.go pulls in
// ProviderSet with one line, so no upstream ProviderSet has to change.
package dpps

import (
	"database/sql"

	"github.com/Wei-Shaw/sub2api/internal/handler"
	"github.com/Wei-Shaw/sub2api/internal/repository"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/google/wire"
)

// DppsSeatRuntime holds the wired Seat API services installed for lazy route handlers and middlewares.
type DppsSeatRuntime struct {
	SeatService      *service.DppsSeatService
	SeatTokenService *service.DppsSeatTokenService
}

// ProviderSet is every provider the DPPS additions need.
var ProviderSet = wire.NewSet(
	repository.NewDppsEventOutboxRepository,
	repository.NewDppsSeatRepository,
	service.NewDppsSeatService,
	service.NewDppsSeatTokenService,
	ProvideDppsSeatRuntime,
	ProvideEventsService,
)

// ProvideDppsSeatRuntime installs the Seat API services and middleware repositories
// so routes mounted earlier in the Wire graph resolve them on every request.
func ProvideDppsSeatRuntime(
	settingRepo service.SettingRepository,
	auditRepo service.AuditLogRepository,
	seatSvc *service.DppsSeatService,
	seatTokenSvc *service.DppsSeatTokenService,
) *DppsSeatRuntime {
	service.SetDppsSeatMiddlewareRepos(settingRepo, auditRepo)
	service.SetDppsSeatInstance(seatSvc)
	service.SetDppsSeatTokenInstance(seatTokenSvc)
	return &DppsSeatRuntime{
		SeatService:      seatSvc,
		SeatTokenService: seatTokenSvc,
	}
}

// ProvideEventsService creates the outgoing events webhook (SC-28), installs it
// as the process-wide event sink and starts its leader-locked delivery worker.
// It does nothing until an admin enables it in the settings. The gateway handler
// is a dependency only so that the webhook can hash the model list /v1/models
// really serves (the handler owns the platform fallbacks and allowlist rules).
func ProvideEventsService(
	settingRepo service.SettingRepository,
	outbox service.DppsEventOutboxRepository,
	encryptor service.SecretEncryptor,
	backup *service.BackupService,
	accounts service.AccountRepository,
	groups service.GroupRepository,
	gateway *service.GatewayService,
	users service.UserRepository,
	pricing *service.PricingService,
	lockCache service.LeaderLockCache,
	db *sql.DB,
	buildInfo service.BuildInfo,
	gatewayHandler *handler.GatewayHandler,
	seatRuntime *DppsSeatRuntime,
) *service.DppsEventsService {
	_ = seatRuntime
	service.SetDppsEventsServedModelsLister(gatewayHandler.DppsServedModelIDs)
	data := service.NewDppsEventsDataSource(accounts, groups, gateway, users, pricing)
	svc := service.NewDppsEventsService(settingRepo, outbox, encryptor, backup.EncryptionKeyConfigured, data, lockCache, db, buildInfo.Version)
	service.SetDppsEventSink(svc)
	svc.Start()
	return svc
}
