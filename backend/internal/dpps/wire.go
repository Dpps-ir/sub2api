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

// ProviderSet is every provider the DPPS additions need.
var ProviderSet = wire.NewSet(
	repository.NewDppsEventOutboxRepository,
	ProvideEventsService,
)

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
) *service.DppsEventsService {
	service.SetDppsEventsServedModelsLister(gatewayHandler.DppsServedModelIDs)
	data := service.NewDppsEventsDataSource(accounts, groups, gateway, users, pricing)
	svc := service.NewDppsEventsService(settingRepo, outbox, encryptor, backup.EncryptionKeyConfigured, data, lockCache, db, buildInfo.Version)
	service.SetDppsEventSink(svc)
	svc.Start()
	return svc
}
