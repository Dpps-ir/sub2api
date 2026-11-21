package service

import (
	"context"
	"encoding/json"
	"reflect"
	"regexp"
	"sort"
	"strings"
	"sync/atomic"
)

// Outgoing events webhook (SC-28, D-248): Sub2API tells one configured receiver
// (DPPS) about every change that matters, signed and retried, through a
// database outbox. Everything here is off until an admin enables it.

// Event type names. Keep in step with docs/contracts/sub2api-events.md.
const (
	DppsEventModelsChanged   = "models.changed"
	DppsEventSeatStatus      = "seat.status"
	DppsEventSeatPaused      = "seat.paused"
	DppsEventSeatResumed     = "seat.resumed"
	DppsEventSeatLimited     = "seat.limited"
	DppsEventSeatCleared     = "seat.cleared"
	DppsEventSeatUsage       = "seat.usage"
	DppsEventSeatCreated     = "seat.created"
	DppsEventSeatUpdated     = "seat.updated"
	DppsEventSeatDeleted     = "seat.deleted"
	DppsEventGroupChanged    = "group.changed"
	DppsEventChannelChanged  = "channel.changed"
	DppsEventPricesChanged   = "prices.changed"
	DppsEventBalanceLow      = "balance.low"
	DppsEventBalanceChanged  = "balance.changed"
	DppsEventKeyChanged      = "key.changed"
	DppsEventSettingsChanged = "settings.changed"
	DppsEventSystemStarted   = "system.started"
	DppsEventVersionChanged  = "system.version_changed"
	DppsEventAlertFired      = "alert.fired"
	DppsEventAlertResolved   = "alert.resolved"
	DppsEventSnapshotDaily   = "snapshot.daily"
)

// DppsAllEventTypes lists every type the sender can emit (admin UI and validation).
var DppsAllEventTypes = []string{
	DppsEventModelsChanged, DppsEventSeatStatus, DppsEventSeatPaused, DppsEventSeatResumed, DppsEventSeatLimited,
	DppsEventSeatCleared, DppsEventSeatUsage, DppsEventSeatCreated, DppsEventSeatUpdated, DppsEventSeatDeleted,
	DppsEventGroupChanged, DppsEventChannelChanged, DppsEventPricesChanged, DppsEventBalanceLow, DppsEventBalanceChanged,
	DppsEventKeyChanged, DppsEventSettingsChanged, DppsEventSystemStarted, DppsEventVersionChanged,
	DppsEventAlertFired, DppsEventAlertResolved, DppsEventSnapshotDaily,
}

func dppsIsKnownEventType(t string) bool {
	for _, k := range DppsAllEventTypes {
		if k == t {
			return true
		}
	}
	return false
}

// DppsEventSink receives events from the emit points. Emit never fails the caller:
// a lost event is covered by the daily snapshot.
type DppsEventSink interface {
	// Wants is a cheap check so emit points skip any extra work when off.
	Wants(eventType string) bool
	Emit(ctx context.Context, eventType string, data map[string]any)
}

type dppsNoopEventSink struct{}

func (dppsNoopEventSink) Wants(string) bool                            { return false }
func (dppsNoopEventSink) Emit(context.Context, string, map[string]any) {}

type dppsEventSinkHolder struct{ sink DppsEventSink }

var dppsGlobalEventSink atomic.Pointer[dppsEventSinkHolder]

// SetDppsEventSink installs the process-wide sink (the DppsEventsService at start, a
// fake in tests). Passing nil restores the no-op sink.
func SetDppsEventSink(s DppsEventSink) {
	if s == nil {
		dppsGlobalEventSink.Store(nil)
		return
	}
	dppsGlobalEventSink.Store(&dppsEventSinkHolder{sink: s})
}

func dppsCurrentEventSink() DppsEventSink {
	if h := dppsGlobalEventSink.Load(); h != nil {
		return h.sink
	}
	return dppsNoopEventSink{}
}

// DppsEventWanted reports whether the sink would take this type.
func DppsEventWanted(eventType string) bool { return dppsCurrentEventSink().Wants(eventType) }

// DppsEmitEvent hands an event to the sink at once.
func DppsEmitEvent(ctx context.Context, eventType string, data map[string]any) {
	s := dppsCurrentEventSink()
	if !s.Wants(eventType) {
		return
	}
	s.Emit(ctx, eventType, data)
}

// DppsEmitEventAfterCommit hands an event to the sink once the surrounding ent
// transaction (if any) has committed; without one it runs at once.
func DppsEmitEventAfterCommit(ctx context.Context, eventType string, data map[string]any) {
	if !DppsEventWanted(eventType) {
		return
	}
	AfterAccountCommit(ctx, func(c context.Context) { DppsEmitEvent(c, eventType, data) })
}

// dppsEventChangedFields lists the names of the non-zero fields of an input struct
// (pointer or value) in snake case. Names only, never values.
func dppsEventChangedFields(input any) []string {
	v := reflect.ValueOf(input)
	for v.IsValid() && (v.Kind() == reflect.Ptr || v.Kind() == reflect.Interface) {
		if v.IsNil() {
			return nil
		}
		v = v.Elem()
	}
	if !v.IsValid() || v.Kind() != reflect.Struct {
		return nil
	}
	t := v.Type()
	out := []string{}
	for i := 0; i < t.NumField(); i++ {
		f := t.Field(i)
		if !f.IsExported() || f.Name == "SkipMixedChannelCheck" || f.Name == "ProxyMode" {
			continue
		}
		if !v.Field(i).IsZero() {
			out = append(out, dppsEventSnakeCase(f.Name))
		}
	}
	sort.Strings(out)
	return out
}

var dppsEventSnakeRe = regexp.MustCompile(`([a-z0-9])([A-Z])`)

func dppsEventSnakeCase(s string) string {
	return strings.ToLower(dppsEventSnakeRe.ReplaceAllString(s, "${1}_${2}"))
}

// Defence in depth for the "never in any event" list of the contract: emit
// points send only safe fields, and this pass drops anything that looks like a
// credential key and masks anything that looks like an email address.
var (
	dppsEventForbiddenKeyParts = []string{"token", "password", "passwd", "secret", "credential", "authorization", "api_key", "apikey", "email", "cookie", "private"}
	dppsEventEmailRe           = regexp.MustCompile(`[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+`)
)

func dppsEventKeyForbidden(key string) bool {
	k := strings.ToLower(key)
	for _, p := range dppsEventForbiddenKeyParts {
		if strings.Contains(k, p) {
			return true
		}
	}
	return false
}

// dppsSanitizeEventData returns a deep copy with forbidden keys removed and email
// addresses masked. It round-trips through JSON so only JSON-safe values remain.
func dppsSanitizeEventData(data map[string]any) map[string]any {
	if data == nil {
		return map[string]any{}
	}
	raw, err := json.Marshal(data)
	if err != nil {
		return map[string]any{}
	}
	var generic map[string]any
	if err := json.Unmarshal(raw, &generic); err != nil {
		return map[string]any{}
	}
	return dppsSanitizeEventValue(generic).(map[string]any)
}

func dppsSanitizeEventValue(v any) any {
	switch x := v.(type) {
	case map[string]any:
		out := make(map[string]any, len(x))
		for k, val := range x {
			if dppsEventKeyForbidden(k) {
				continue
			}
			out[k] = dppsSanitizeEventValue(val)
		}
		return out
	case []any:
		out := make([]any, len(x))
		for i, val := range x {
			out[i] = dppsSanitizeEventValue(val)
		}
		return out
	case string:
		return dppsEventEmailRe.ReplaceAllString(x, "[email]")
	default:
		return v
	}
}
