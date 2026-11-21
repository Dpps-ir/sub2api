package service

import (
	"context"
	"fmt"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"
)

// Emit helpers used by the emit points. Each builds a small, credential-free
// payload; account names are added by the EventsService when it enqueues.

func seatData(id int64, extra map[string]any) map[string]any {
	d := map[string]any{"account_id": id}
	for k, v := range extra {
		d[k] = v
	}
	return d
}

var (
	reasonRefreshRe  = regexp.MustCompile(`(?i)refresh`)
	reasonAuthRe     = regexp.MustCompile(`(?i)\b401\b|unauthori[sz]ed|invalid[_ ](token|grant|credential)|authenticat`)
	reasonForbiddRe  = regexp.MustCompile(`(?i)\b403\b|forbidden|permission`)
	reasonUpstreamRe = regexp.MustCompile(`\b[45]\d\d\b`)
)

// SeatErrorReasonClass reduces a raw error message to a class. The raw text
// (which can hold vendor detail) never leaves Sub2API.
func SeatErrorReasonClass(msg string) string {
	switch {
	case reasonRefreshRe.MatchString(msg):
		return "refresh_failed"
	case reasonAuthRe.MatchString(msg):
		return "auth"
	case reasonForbiddRe.MatchString(msg):
		return "forbidden"
	case reasonUpstreamRe.MatchString(msg):
		return "upstream_code"
	default:
		return "other"
	}
}

// EmitSeatStatus: an account went to error (reasonClass set) or back to active.
func EmitSeatStatus(ctx context.Context, id int64, status, reasonClass string) {
	d := map[string]any{"status": status}
	if reasonClass != "" {
		d["reason"] = reasonClass
	}
	EmitEventAfterCommit(ctx, EventSeatStatus, seatData(id, d))
}

// EmitSeatSchedulable: schedulable switched off or on. by is "admin" or "expiry".
func EmitSeatSchedulable(ctx context.Context, id int64, schedulable bool, by string) {
	t := EventSeatResumed
	if !schedulable {
		t = EventSeatPaused
	}
	EmitEventAfterCommit(ctx, t, seatData(id, map[string]any{"by": by}))
}

// EmitSeatLimited: rate limited (429), overloaded (529) or temporarily unschedulable.
// kind is "rate_limit", "overload" or "temp_unschedulable"; window is "5h", "7d" or "".
func EmitSeatLimited(ctx context.Context, id int64, kind string, until time.Time, window string) {
	d := map[string]any{"kind": kind, "until": until.UTC().Format(time.RFC3339)}
	if window != "" {
		d["window"] = window
	}
	d[eventKeyField] = fmt.Sprintf("%d|%s|%s", id, kind, until.UTC().Truncate(time.Minute).Format(time.RFC3339))
	EmitEventAfterCommit(ctx, EventSeatLimited, seatData(id, d))
}

// EmitSeatCleared: an explicit clear of a limit.
func EmitSeatCleared(ctx context.Context, id int64, kind string) {
	EmitEventAfterCommit(ctx, EventSeatCleared, seatData(id, map[string]any{"kind": kind}))
}

// EmitSeatLifecycle: seat.created / seat.updated / seat.deleted from admin edits.
func EmitSeatLifecycle(ctx context.Context, eventType string, id int64, name, platform string, priority *int, changedFields []string) {
	d := map[string]any{}
	if name != "" {
		d["account_name"] = name
	}
	if platform != "" {
		d["platform"] = platform
	}
	if priority != nil {
		d["priority"] = *priority
	}
	if len(changedFields) > 0 {
		d["changed_fields"] = changedFields
	}
	EmitEventAfterCommit(ctx, eventType, seatData(id, d))
	EventsModelsMayHaveChanged()
}

// ---- seat.usage -------------------------------------------------------------

var seatUsageLevels = []float64{100, 90, 70, 50}

func seatUsageLevel(p float64) int {
	for _, l := range seatUsageLevels {
		if p >= l {
			return int(l)
		}
	}
	return 0
}

type seatWindowState struct {
	seen    bool
	percent float64
	level   int
	resetAt time.Time
}

type seatUsageTracker struct {
	mu    sync.Mutex
	seats map[int64]*[2]seatWindowState // [0]=5h, [1]=7d
}

var defaultSeatUsageTracker = &seatUsageTracker{seats: map[int64]*[2]seatWindowState{}}

func extraFloat(m map[string]any, key string) (float64, bool) {
	switch v := m[key].(type) {
	case float64:
		return v, true
	case float32:
		return float64(v), true
	case int:
		return float64(v), true
	case int64:
		return float64(v), true
	}
	return 0, false
}

func extraTime(m map[string]any, key string) time.Time {
	if s, ok := m[key].(string); ok {
		if t, err := time.Parse(time.RFC3339, s); err == nil {
			return t
		}
	}
	return time.Time{}
}

type seatCrossing struct {
	Window string
	Level  int
	Reason string
}

// observe records the new readings and returns the crossings (up through 50, 70,
// 90 or 100 percent) and window resets. The first reading of a seat only seeds
// the state: the daily snapshot carries the full picture.
func (t *seatUsageTracker) observe(id int64, updates map[string]any) []seatCrossing {
	t.mu.Lock()
	defer t.mu.Unlock()
	st := t.seats[id]
	if st == nil {
		st = &[2]seatWindowState{}
		t.seats[id] = st
	}
	var out []seatCrossing
	for i, w := range []struct{ name, pk, rk string }{
		{"5h", "codex_5h_used_percent", "codex_5h_reset_at"},
		{"7d", "codex_7d_used_percent", "codex_7d_reset_at"},
	} {
		p, ok := extraFloat(updates, w.pk)
		if !ok {
			continue
		}
		reset := extraTime(updates, w.rk)
		lvl := seatUsageLevel(p)
		prev := &st[i]
		if prev.seen {
			switch {
			case lvl > prev.level:
				out = append(out, seatCrossing{Window: w.name, Level: lvl, Reason: "crossing"})
			case p < prev.percent && !reset.IsZero() && reset.Sub(prev.resetAt) >= time.Minute:
				out = append(out, seatCrossing{Window: w.name, Level: lvl, Reason: "reset"})
			}
		}
		prev.seen, prev.percent, prev.level = true, p, lvl
		if !reset.IsZero() {
			prev.resetAt = reset
		}
	}
	return out
}

// EmitSeatUsageFromExtra is called after the codex usage snapshot is written
// (not per write: only a crossing or a window reset produces an event).
func EmitSeatUsageFromExtra(ctx context.Context, accountID int64, updates map[string]any) {
	crossings := defaultSeatUsageTracker.observe(accountID, updates)
	if len(crossings) == 0 || !EventWanted(EventSeatUsage) {
		return
	}
	d := map[string]any{}
	if v, ok := extraFloat(updates, "codex_5h_used_percent"); ok {
		d["used_percent_5h"] = v
	}
	if v, ok := updates["codex_5h_reset_at"]; ok {
		d["reset_at_5h"] = v
	}
	if v, ok := extraFloat(updates, "codex_7d_used_percent"); ok {
		d["used_percent_7d"] = v
	}
	if v, ok := updates["codex_7d_reset_at"]; ok {
		d["reset_at_7d"] = v
	}
	list := make([]map[string]any, 0, len(crossings))
	for _, c := range crossings {
		list = append(list, map[string]any{"window": c.Window, "level": c.Level, "reason": c.Reason})
	}
	d["crossed"] = list
	keys := make([]string, 0, len(crossings))
	for _, c := range crossings {
		keys = append(keys, fmt.Sprintf("%s:%d:%s", c.Window, c.Level, c.Reason))
	}
	reset5, _ := updates["codex_5h_reset_at"].(string)
	d[eventKeyField] = fmt.Sprintf("%d|%s|%s", accountID, strings.Join(keys, ","), minuteOf(reset5))
	EmitEvent(ctx, EventSeatUsage, seatData(accountID, d))
}

// ---- other emit points --------------------------------------------------------

// EmitChangedEntity: group.changed / channel.changed from an admin edit.
// change is "created", "updated" or "deleted"; fields are field names only.
func EmitChangedEntity(ctx context.Context, eventType string, id int64, name, change string, fields []string, extra map[string]any) {
	d := map[string]any{"id": id, "name": name, "change": change}
	if len(fields) > 0 {
		d["changed_fields"] = fields
	}
	for k, v := range extra {
		d[k] = v
	}
	EmitEventAfterCommit(ctx, eventType, d)
	EventsModelsMayHaveChanged()
}

// EmitBalanceChanged: an admin set or adjusted a user's balance.
func EmitBalanceChanged(ctx context.Context, userID int64, oldBalance, newBalance float64, op string) {
	if !EventWanted(EventBalanceChanged) {
		return
	}
	w, ok := currentEventSink().(eventsBalanceWatcher)
	if ok {
		if _, watched := w.BalanceWatch(userID); !watched {
			return
		}
	}
	EmitEventAfterCommit(ctx, EventBalanceChanged, map[string]any{"user_id": userID, "balance": newBalance, "previous_balance": oldBalance, "op": op})
	EmitBalanceLowIfCrossed(ctx, userID, oldBalance, newBalance)
}

// EmitKeyChanged: one of the service user's keys was disabled, expired, or ran out of quota.
// Only keys of the watched users (the service user DPPS holds) are reported.
func EmitKeyChanged(ctx context.Context, userID, keyID int64, name, status string, expiresAt *time.Time) {
	if !EventWanted(EventKeyChanged) {
		return
	}
	if w, ok := currentEventSink().(eventsBalanceWatcher); ok {
		if _, watched := w.BalanceWatch(userID); !watched {
			return
		}
	}
	d := map[string]any{"key_id": keyID, "name": name, "status": status}
	if expiresAt != nil {
		d["expires_at"] = expiresAt.UTC().Format(time.RFC3339)
	}
	EmitEventAfterCommit(ctx, EventKeyChanged, d)
}

// EmitSettingsChanged lists key names only: settings hold secrets, so values never leave.
func EmitSettingsChanged(ctx context.Context, keys []string) {
	if len(keys) == 0 {
		return
	}
	k := append([]string(nil), keys...)
	sort.Strings(k)
	EmitEvent(ctx, EventSettingsChanged, map[string]any{"changed_keys": k})
}

// settingsChangedKeys returns the keys of updates whose stored value differs.
func settingsChangedKeys(old, updates map[string]string) []string {
	var out []string
	for k, v := range updates {
		if strings.HasPrefix(k, "events_webhook_") {
			continue
		}
		if ov, ok := old[k]; !ok || ov != v {
			out = append(out, k)
		}
	}
	sort.Strings(out)
	return out
}

// EmitAlert: Sub2API's own alert rules fired or resolved.
func EmitAlert(ctx context.Context, fired bool, ruleID int64, ruleName, severity, metric string, value, threshold *float64, dims map[string]any) {
	t := EventAlertResolved
	if fired {
		t = EventAlertFired
	}
	d := map[string]any{"rule_id": ruleID, "rule": ruleName, "severity": severity, "metric": metric}
	if value != nil {
		d["value"] = *value
	}
	if threshold != nil {
		d["threshold"] = *threshold
	}
	if v, ok := dims["group_id"]; ok {
		d["group"] = v
	}
	if v, ok := dims["platform"]; ok {
		d["platform"] = v
	}
	EmitEvent(ctx, t, d)
}

// emitPricesChanged reports a price table reload that changed any model's
// per-token input or output price. A first load (no old table) is not a change.
func emitPricesChanged(source string, oldData, newData map[string]*LiteLLMModelPricing, hash string) {
	if len(oldData) == 0 || !EventWanted(EventPricesChanged) {
		return
	}
	names := make([]string, 0, len(newData))
	for name := range newData {
		names = append(names, name)
	}
	sort.Strings(names)
	changes := []map[string]any{}
	total := 0
	for _, name := range names {
		n, o := newData[name], oldData[name]
		if n == nil {
			continue
		}
		var oi, oo float64
		if o != nil {
			oi, oo = o.InputCostPerToken, o.OutputCostPerToken
		}
		if o != nil && oi == n.InputCostPerToken && oo == n.OutputCostPerToken {
			continue
		}
		total++
		if len(changes) < 200 {
			changes = append(changes, map[string]any{
				"model":     name,
				"old_input": oi, "new_input": n.InputCostPerToken,
				"old_output": oo, "new_output": n.OutputCostPerToken,
			})
		}
	}
	if total == 0 {
		return
	}
	EmitEvent(context.Background(), EventPricesChanged, map[string]any{
		"source": source, "hash": hash, "models_changed": total, "models": changes,
	})
}

func minuteOf(rfc3339 string) string {
	if ts, err := time.Parse(time.RFC3339, rfc3339); err == nil {
		return ts.UTC().Truncate(time.Minute).Format(time.RFC3339)
	}
	return ""
}
