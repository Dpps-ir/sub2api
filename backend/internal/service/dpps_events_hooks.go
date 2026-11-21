package service

import (
	"context"
	"strings"
)

// DPPS hook helpers (events webhook, SC-28).
//
// Upstream files carry one-line calls (marked "// DPPS hook") into this file, so
// a rebase onto a new upstream main only has to re-find those lines. All logic
// lives here. See DPPS.md at the repository root for the list of hook sites.

// ---- accounts (admin_account.go) ---------------------------------------------

func (s *adminServiceImpl) dppsAfterAccountCreate(ctx context.Context, account *Account) {
	DppsEmitSeatLifecycle(ctx, DppsEventSeatCreated, account.ID, account.Name, account.Platform, &account.Priority, nil)
}

func (s *adminServiceImpl) dppsAfterAccountUpdate(ctx context.Context, updated *Account, input *UpdateAccountInput) {
	DppsEmitSeatLifecycle(ctx, DppsEventSeatUpdated, updated.ID, updated.Name, updated.Platform, &updated.Priority, dppsEventChangedFields(input))
}

// dppsAfterAccountBulkUpdate reports every account the bulk edit changed (at most
// 500). fields were captured before the update body ran, because the body fills
// and sanitises parts of the input.
func (s *adminServiceImpl) dppsAfterAccountBulkUpdate(ctx context.Context, result *BulkUpdateAccountsResult, fields []string) {
	if result == nil || !DppsEventWanted(DppsEventSeatUpdated) {
		return
	}
	for i, id := range result.SuccessIDs {
		if i >= 500 {
			break
		}
		DppsEmitSeatLifecycle(ctx, DppsEventSeatUpdated, id, "", "", nil, fields)
	}
}

// dppsBeforeAccountDelete reads the name while the row still exists and returns
// the function that reports the deletion once it has succeeded.
func (s *adminServiceImpl) dppsBeforeAccountDelete(ctx context.Context, id int64) func() {
	name, platform := "", ""
	if DppsEventWanted(DppsEventSeatDeleted) {
		if acc, err := s.accountRepo.GetByID(ctx, id); err == nil && acc != nil {
			name, platform = acc.Name, acc.Platform
		}
	}
	return func() { DppsEmitSeatLifecycle(ctx, DppsEventSeatDeleted, id, name, platform, nil, nil) }
}

// ---- groups (admin_group.go) -------------------------------------------------

// dppsGroupEventExtra is the non-secret model routing detail a group.changed event carries.
func dppsGroupEventExtra(g *Group) map[string]any {
	d := map[string]any{"platform": g.Platform}
	if len(g.SupportedModelScopes) > 0 {
		d["supported_model_scopes"] = g.SupportedModelScopes
	}
	if g.DefaultMappedModel != "" {
		d["default_mapped_model"] = g.DefaultMappedModel
	}
	return d
}

func (s *adminServiceImpl) dppsAfterGroupCreate(ctx context.Context, group *Group) {
	if group != nil {
		DppsEmitChangedEntity(ctx, DppsEventGroupChanged, group.ID, group.Name, "created", nil, dppsGroupEventExtra(group))
	}
}

func (s *adminServiceImpl) dppsAfterGroupUpdate(ctx context.Context, group *Group, input *UpdateGroupInput) {
	if group != nil {
		DppsEmitChangedEntity(ctx, DppsEventGroupChanged, group.ID, group.Name, "updated", dppsEventChangedFields(input), dppsGroupEventExtra(group))
	}
}

// dppsBeforeGroupDelete reads the name before the row is gone and returns the
// function that reports the deletion once it has succeeded.
func (s *adminServiceImpl) dppsBeforeGroupDelete(ctx context.Context, id int64) func() {
	name := ""
	if DppsEventWanted(DppsEventGroupChanged) {
		if g, err := s.groupRepo.GetByIDLite(ctx, id); err == nil && g != nil {
			name = g.Name
		}
	}
	return func() { DppsEmitChangedEntity(ctx, DppsEventGroupChanged, id, name, "deleted", nil, nil) }
}

// ---- channels (channel_service.go) -------------------------------------------

// dppsChannelEventExtra is the non-secret mapping and allowlist detail a channel.changed event carries.
func dppsChannelEventExtra(ch *Channel) map[string]any {
	d := map[string]any{"status": ch.Status, "restrict_models": ch.RestrictModels, "group_ids": ch.GroupIDs}
	if len(ch.ModelMapping) > 0 {
		d["model_mapping"] = ch.ModelMapping
	}
	return d
}

func (s *ChannelService) dppsAfterChannelCreate(ctx context.Context, ch *Channel) {
	if ch != nil {
		DppsEmitChangedEntity(ctx, DppsEventChannelChanged, ch.ID, ch.Name, "created", nil, dppsChannelEventExtra(ch))
	}
}

func (s *ChannelService) dppsAfterChannelUpdate(ctx context.Context, ch *Channel, input *UpdateChannelInput) {
	if ch != nil {
		DppsEmitChangedEntity(ctx, DppsEventChannelChanged, ch.ID, ch.Name, "updated", dppsEventChangedFields(input), dppsChannelEventExtra(ch))
	}
}

func (s *ChannelService) dppsBeforeChannelDelete(ctx context.Context, id int64) func() {
	name := ""
	if DppsEventWanted(DppsEventChannelChanged) {
		if ch, err := s.repo.GetByID(ctx, id); err == nil && ch != nil {
			name = ch.Name
		}
	}
	return func() { DppsEmitChangedEntity(ctx, DppsEventChannelChanged, id, name, "deleted", nil, nil) }
}

// ---- settings (setting_update.go) --------------------------------------------

// dppsSetMultiple replaces settingRepo.SetMultiple in the two settings update
// paths: it writes the settings and, when the events webhook wants
// settings.changed, reports the names of the keys whose value differs. Values
// never leave: settings hold secrets.
func (s *SettingService) dppsSetMultiple(ctx context.Context, updates map[string]string) error {
	var old map[string]string
	if DppsEventWanted(DppsEventSettingsChanged) {
		keys := make([]string, 0, len(updates))
		for k := range updates {
			keys = append(keys, k)
		}
		if got, err := s.settingRepo.GetMultiple(ctx, keys); err == nil {
			old = got
		}
	}
	if err := s.settingRepo.SetMultiple(ctx, updates); err != nil {
		return err
	}
	if old != nil {
		DppsEmitSettingsChanged(ctx, dppsSettingsChangedKeys(old, updates))
	}
	return nil
}

// ---- pricing (pricing_service.go) --------------------------------------------

// DppsPriceTableHash returns the hash of the loaded price table (full hex).
func (s *PricingService) DppsPriceTableHash() string {
	if s == nil {
		return ""
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.localHash + s.customFilesHash
}

// dppsPricesCustomReloaded: the custom price files were reloaded.
func (s *PricingService) dppsPricesCustomReloaded(oldData, newData map[string]*LiteLLMModelPricing) {
	dppsEmitPricesChanged("custom", oldData, newData, s.DppsPriceTableHash())
}

// dppsPricesLoaded: the price table was loaded from filePath (remote copy or fallback file).
func (s *PricingService) dppsPricesLoaded(filePath string, oldData, newData map[string]*LiteLLMModelPricing, hash string) {
	source := "remote"
	if s.cfg != nil && s.cfg.Pricing.FallbackFile != "" && filePath == s.cfg.Pricing.FallbackFile {
		source = "fallback"
	}
	dppsEmitPricesChanged(source, oldData, newData, hash)
}

// ---- other one-line helpers ---------------------------------------------------

// DppsEmitBalanceChangedIfDiff: an admin set or adjusted a user's balance (admin_user.go).
func DppsEmitBalanceChangedIfDiff(ctx context.Context, userID int64, oldBalance, newBalance float64, op string) {
	if newBalance != oldBalance {
		DppsEmitBalanceChanged(ctx, userID, oldBalance, newBalance, op)
	}
}

// DppsEmitKeyUpdated reports a key whose status changed or whose expiry was edited (api_key_service.go).
func DppsEmitKeyUpdated(ctx context.Context, key *APIKey, originalStatus string, expiryEdited bool) {
	if key.Status != originalStatus || expiryEdited {
		DppsEmitKeyChanged(ctx, key.UserID, key.ID, key.Name, key.Status, key.ExpiresAt)
	}
}

// DppsEmitAlertFromEvent reports an Ops alert rule that fired or resolved (ops_alert_evaluator_service.go).
func DppsEmitAlertFromEvent(ctx context.Context, fired bool, rule *OpsAlertRule, ev *OpsAlertEvent) {
	DppsEmitAlert(ctx, fired, rule.ID, strings.TrimSpace(rule.Name), strings.TrimSpace(rule.Severity), rule.MetricType, ev.MetricValue, ev.ThresholdValue, ev.Dimensions)
}

// DppsAfterBillingApply runs after a usage billing transaction committed: a watched
// balance falling below its level and a key running out of quota (both rare,
// never per request) (usage_billing_repo.go).
func DppsAfterBillingApply(ctx context.Context, cmd *UsageBillingCommand, result *UsageBillingApplyResult) {
	if result == nil {
		return
	}
	if result.NewBalance != nil {
		DppsEmitBalanceLowIfCrossed(ctx, cmd.UserID, *result.NewBalance+cmd.BalanceCost, *result.NewBalance)
	}
	if result.APIKeyQuotaExhausted {
		DppsEmitKeyChanged(ctx, cmd.UserID, cmd.APIKeyID, "", StatusAPIKeyQuotaExhausted, nil)
	}
}

// ---- seats (repository/account_repo.go) ---------------------------------------

// DppsEmitSeatError: an account went to error. wasError is the status read before the
// write: seat.status is a transition, so a repeated SetError emits nothing.
func DppsEmitSeatError(ctx context.Context, wasError bool, id int64, errorMsg string) {
	if !wasError {
		DppsEmitSeatStatus(ctx, id, StatusError, DppsSeatErrorReasonClass(errorMsg))
	}
}

// DppsEmitSeatRecovered: ClearError on an account that was in error.
func DppsEmitSeatRecovered(ctx context.Context, wasError bool, id int64) {
	if wasError {
		DppsEmitSeatStatus(ctx, id, StatusActive, "")
	}
}

// DppsEmitSeatClearedIf: a limit or pause was cleared on a seat that held one.
func DppsEmitSeatClearedIf(ctx context.Context, held bool, id int64, kind string) {
	if held {
		DppsEmitSeatCleared(ctx, id, kind)
	}
}

// DppsEmitSeatsAutoPaused: the expiry sweep paused these accounts.
func DppsEmitSeatsAutoPaused(ctx context.Context, accountIDs []int64) {
	for _, id := range accountIDs {
		DppsEmitSeatSchedulable(ctx, id, false, "expiry")
	}
}

// DppsEventsInstance returns the running events service, or nil when none was
// installed (unit tests that build the routes without the dependency graph).
func DppsEventsInstance() *DppsEventsService {
	s, _ := dppsCurrentEventSink().(*DppsEventsService)
	return s
}
