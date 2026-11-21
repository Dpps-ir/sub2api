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
	EventModelsChanged   = "models.changed"
	EventSeatStatus      = "seat.status"
	EventSeatPaused      = "seat.paused"
	EventSeatResumed     = "seat.resumed"
	EventSeatLimited     = "seat.limited"
	EventSeatCleared     = "seat.cleared"
	EventSeatUsage       = "seat.usage"
	EventSeatCreated     = "seat.created"
	EventSeatUpdated     = "seat.updated"
	EventSeatDeleted     = "seat.deleted"
	EventGroupChanged    = "group.changed"
	EventChannelChanged  = "channel.changed"
	EventPricesChanged   = "prices.changed"
	EventBalanceLow      = "balance.low"
	EventBalanceChanged  = "balance.changed"
	EventKeyChanged      = "key.changed"
	EventSettingsChanged = "settings.changed"
	EventSystemStarted   = "system.started"
	EventVersionChanged  = "system.version_changed"
	EventAlertFired      = "alert.fired"
	EventAlertResolved   = "alert.resolved"
	EventSnapshotDaily   = "snapshot.daily"
)

// AllEventTypes lists every type the sender can emit (admin UI and validation).
var AllEventTypes = []string{
	EventModelsChanged, EventSeatStatus, EventSeatPaused, EventSeatResumed, EventSeatLimited,
	EventSeatCleared, EventSeatUsage, EventSeatCreated, EventSeatUpdated, EventSeatDeleted,
	EventGroupChanged, EventChannelChanged, EventPricesChanged, EventBalanceLow, EventBalanceChanged,
	EventKeyChanged, EventSettingsChanged, EventSystemStarted, EventVersionChanged,
	EventAlertFired, EventAlertResolved, EventSnapshotDaily,
}

func isKnownEventType(t string) bool {
	for _, k := range AllEventTypes {
		if k == t {
			return true
		}
	}
	return false
}

// EventSink receives events from the emit points. Emit never fails the caller:
// a lost event is covered by the daily snapshot.
type EventSink interface {
	// Wants is a cheap check so emit points skip any extra work when off.
	Wants(eventType string) bool
	Emit(ctx context.Context, eventType string, data map[string]any)
}

type noopEventSink struct{}

func (noopEventSink) Wants(string) bool                            { return false }
func (noopEventSink) Emit(context.Context, string, map[string]any) {}

type eventSinkHolder struct{ sink EventSink }

var globalEventSink atomic.Pointer[eventSinkHolder]

// SetEventSink installs the process-wide sink (the EventsService at start, a
// fake in tests). Passing nil restores the no-op sink.
func SetEventSink(s EventSink) {
	if s == nil {
		globalEventSink.Store(nil)
		return
	}
	globalEventSink.Store(&eventSinkHolder{sink: s})
}

func currentEventSink() EventSink {
	if h := globalEventSink.Load(); h != nil {
		return h.sink
	}
	return noopEventSink{}
}

// EventWanted reports whether the sink would take this type.
func EventWanted(eventType string) bool { return currentEventSink().Wants(eventType) }

// EmitEvent hands an event to the sink at once.
func EmitEvent(ctx context.Context, eventType string, data map[string]any) {
	s := currentEventSink()
	if !s.Wants(eventType) {
		return
	}
	s.Emit(ctx, eventType, data)
}

// EmitEventAfterCommit hands an event to the sink once the surrounding ent
// transaction (if any) has committed; without one it runs at once.
func EmitEventAfterCommit(ctx context.Context, eventType string, data map[string]any) {
	if !EventWanted(eventType) {
		return
	}
	AfterAccountCommit(ctx, func(c context.Context) { EmitEvent(c, eventType, data) })
}

// eventChangedFields lists the names of the non-zero fields of an input struct
// (pointer or value) in snake case. Names only, never values.
func eventChangedFields(input any) []string {
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
			out = append(out, eventSnakeCase(f.Name))
		}
	}
	sort.Strings(out)
	return out
}

var eventSnakeRe = regexp.MustCompile(`([a-z0-9])([A-Z])`)

func eventSnakeCase(s string) string {
	return strings.ToLower(eventSnakeRe.ReplaceAllString(s, "${1}_${2}"))
}

// Defence in depth for the "never in any event" list of the contract: emit
// points send only safe fields, and this pass drops anything that looks like a
// credential key and masks anything that looks like an email address.
var (
	eventForbiddenKeyParts = []string{"token", "password", "passwd", "secret", "credential", "authorization", "api_key", "apikey", "email", "cookie", "private"}
	eventEmailRe           = regexp.MustCompile(`[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+`)
)

func eventKeyForbidden(key string) bool {
	k := strings.ToLower(key)
	for _, p := range eventForbiddenKeyParts {
		if strings.Contains(k, p) {
			return true
		}
	}
	return false
}

// sanitizeEventData returns a deep copy with forbidden keys removed and email
// addresses masked. It round-trips through JSON so only JSON-safe values remain.
func sanitizeEventData(data map[string]any) map[string]any {
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
	return sanitizeEventValue(generic).(map[string]any)
}

func sanitizeEventValue(v any) any {
	switch x := v.(type) {
	case map[string]any:
		out := make(map[string]any, len(x))
		for k, val := range x {
			if eventKeyForbidden(k) {
				continue
			}
			out[k] = sanitizeEventValue(val)
		}
		return out
	case []any:
		out := make([]any, len(x))
		for i, val := range x {
			out[i] = sanitizeEventValue(val)
		}
		return out
	case string:
		return eventEmailRe.ReplaceAllString(x, "[email]")
	default:
		return v
	}
}
