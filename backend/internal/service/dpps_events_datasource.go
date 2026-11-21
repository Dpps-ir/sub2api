package service

import (
	"context"
	"sync/atomic"
	"time"
)

// DppsEventsServedModelsLister returns the model ids GET /v1/models serves a key of
// the group. The handler layer registers it (it owns the platform fallbacks and
// allowlist rules); until then the account-derived list is used.
type DppsEventsServedModelsLister func(ctx context.Context, group *Group) []string

var dppsEventsServedModelsLister atomic.Pointer[DppsEventsServedModelsLister]

// SetDppsEventsServedModelsLister registers the served-list function.
func SetDppsEventsServedModelsLister(fn DppsEventsServedModelsLister) {
	if fn == nil {
		dppsEventsServedModelsLister.Store(nil)
		return
	}
	dppsEventsServedModelsLister.Store(&fn)
}

// dppsEventsDataSource reads the state the events worker reports. It is read-only.
type dppsEventsDataSource struct {
	accounts AccountRepository
	groups   GroupRepository
	gateway  *GatewayService
	users    UserRepository
	pricing  *PricingService
}

func NewDppsEventsDataSource(accounts AccountRepository, groups GroupRepository, gateway *GatewayService, users UserRepository, pricing *PricingService) DppsEventsDataSource {
	return &dppsEventsDataSource{accounts: accounts, groups: groups, gateway: gateway, users: users, pricing: pricing}
}

func (d *dppsEventsDataSource) AccountBrief(ctx context.Context, id int64) (map[string]any, bool) {
	acc, err := d.accounts.GetByID(ctx, id)
	if err != nil || acc == nil {
		return nil, false
	}
	return map[string]any{"account_name": acc.Name, "platform": acc.Platform, "priority": acc.Priority}, true
}

func (d *dppsEventsDataSource) GroupModelLists(ctx context.Context) ([]DppsEventGroupModels, error) {
	groups, err := d.groups.ListActive(ctx)
	if err != nil {
		return nil, err
	}
	out := make([]DppsEventGroupModels, 0, len(groups))
	for i := range groups {
		g := groups[i]
		id := g.ID
		var models []string
		if fn := dppsEventsServedModelsLister.Load(); fn != nil {
			models = (*fn)(ctx, &g)
		} else {
			models = d.gateway.GetAvailableModels(ctx, &id, g.Platform)
		}
		out = append(out, DppsEventGroupModels{GroupID: g.ID, Name: g.Name, Models: models})
	}
	return out, nil
}

func (d *dppsEventsDataSource) SeatSnapshots(ctx context.Context) ([]map[string]any, error) {
	accs, err := d.accounts.ListAllWithFilters(ctx, "", "", "", "", 0, "")
	if err != nil {
		return nil, err
	}
	now := time.Now()
	out := make([]map[string]any, 0, len(accs))
	for i := range accs {
		a := &accs[i]
		seat := map[string]any{
			"account_id": a.ID, "account_name": a.Name, "platform": a.Platform,
			"status": a.Status, "schedulable": a.Schedulable, "priority": a.Priority,
		}
		if a.RateLimitResetAt != nil && a.RateLimitResetAt.After(now) {
			seat["rate_limited_until"] = a.RateLimitResetAt.UTC().Format(time.RFC3339)
		}
		if a.OverloadUntil != nil && a.OverloadUntil.After(now) {
			seat["overloaded_until"] = a.OverloadUntil.UTC().Format(time.RFC3339)
		}
		if a.TempUnschedulableUntil != nil && a.TempUnschedulableUntil.After(now) {
			seat["temp_unschedulable_until"] = a.TempUnschedulableUntil.UTC().Format(time.RFC3339)
		}
		if v, ok := dppsExtraFloat(a.Extra, "codex_5h_used_percent"); ok {
			seat["used_percent_5h"] = v
			if r, ok := a.Extra["codex_5h_reset_at"]; ok {
				seat["reset_at_5h"] = r
			}
		}
		if v, ok := dppsExtraFloat(a.Extra, "codex_7d_used_percent"); ok {
			seat["used_percent_7d"] = v
			if r, ok := a.Extra["codex_7d_reset_at"]; ok {
				seat["reset_at_7d"] = r
			}
		}
		out = append(out, seat)
	}
	return out, nil
}

func (d *dppsEventsDataSource) UserBalance(ctx context.Context, userID int64) (float64, bool) {
	u, err := d.users.GetByID(ctx, userID)
	if err != nil || u == nil {
		return 0, false
	}
	return u.Balance, true
}

func (d *dppsEventsDataSource) PriceTableHash() string {
	if d.pricing == nil {
		return ""
	}
	return d.pricing.DppsPriceTableHash()
}
