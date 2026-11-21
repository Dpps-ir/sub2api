//go:build unit

package service

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

// The upstream files call these helpers from one-line hooks; these tests pin what
// each helper reports, so a hook moved during a rebase cannot silently change it.
func TestDppsHookHelpers_ReportWhatTheOldInlineEmitPointsDid(t *testing.T) {
	sink := &dppsEvtSink{watched: map[int64]float64{7: 5}}
	dppsInstallEvtSink(t, sink)
	ctx := context.Background()
	admin := &adminServiceImpl{}
	channels := &ChannelService{}

	admin.dppsAfterGroupCreate(ctx, &Group{ID: 5, Name: "g", Platform: PlatformOpenAI, DefaultMappedModel: "m"})
	admin.dppsAfterGroupUpdate(ctx, &Group{ID: 5, Name: "g", Platform: PlatformOpenAI}, &UpdateGroupInput{Name: "g2"})
	admin.dppsAfterGroupCreate(ctx, nil)
	ch := &Channel{ID: 6, Name: "c", Status: "active", GroupIDs: []int64{5}, ModelMapping: map[string]map[string]string{"openai": {"a": "b"}}}
	channels.dppsAfterChannelCreate(ctx, ch)
	channels.dppsAfterChannelUpdate(ctx, ch, &UpdateChannelInput{Name: "c2"})

	groups := sink.byType(DppsEventGroupChanged)
	require.Len(t, groups, 2)
	require.Equal(t, "created", groups[0].Data["change"])
	require.Equal(t, "m", groups[0].Data["default_mapped_model"])
	require.Equal(t, "updated", groups[1].Data["change"])
	require.Equal(t, []string{"name"}, groups[1].Data["changed_fields"])
	chans := sink.byType(DppsEventChannelChanged)
	require.Len(t, chans, 2)
	require.Equal(t, "created", chans[0].Data["change"])
	require.Contains(t, chans[0].Data, "model_mapping")
	require.Equal(t, []string{"name"}, chans[1].Data["changed_fields"])

	// Account bulk edit: names captured up front, one event per changed seat.
	admin.dppsAfterAccountBulkUpdate(ctx, &BulkUpdateAccountsResult{SuccessIDs: []int64{1, 2}}, []string{"priority"})
	admin.dppsAfterAccountBulkUpdate(ctx, nil, nil)
	require.Len(t, sink.byType(DppsEventSeatUpdated), 2)
	admin.dppsAfterAccountCreate(ctx, &Account{ID: 9, Name: "a", Platform: PlatformOpenAI, Priority: 3})
	require.Len(t, sink.byType(DppsEventSeatCreated), 1)

	// A key: only a status change or an expiry edit is reported, only for a watched user.
	key := &APIKey{ID: 70, UserID: 7, Name: "k", Status: StatusAPIKeyQuotaExhausted}
	DppsEmitKeyUpdated(ctx, key, StatusAPIKeyActive, false)
	DppsEmitKeyUpdated(ctx, key, StatusAPIKeyQuotaExhausted, false)
	DppsEmitKeyUpdated(ctx, key, StatusAPIKeyQuotaExhausted, true)
	require.Len(t, sink.byType(DppsEventKeyChanged), 2)

	// Balance: an equal balance is not a change.
	DppsEmitBalanceChangedIfDiff(ctx, 7, 10, 10, "add")
	require.Empty(t, sink.byType(DppsEventBalanceChanged))
	DppsEmitBalanceChangedIfDiff(ctx, 7, 10, 12, "add")
	require.Len(t, sink.byType(DppsEventBalanceChanged), 1)

	// Alerts take their numbers from the event row.
	rule := &OpsAlertRule{ID: 3, Name: " high error rate ", Severity: " P1 ", MetricType: "error_rate"}
	ev := &OpsAlertEvent{MetricValue: dppsF64(12), ThresholdValue: dppsF64(5), Dimensions: map[string]any{"group_id": int64(2)}}
	DppsEmitAlertFromEvent(ctx, true, rule, ev)
	DppsEmitAlertFromEvent(ctx, false, rule, ev)
	fired := sink.byType(DppsEventAlertFired)
	require.Len(t, fired, 1)
	require.Equal(t, "high error rate", fired[0].Data["rule"])
	require.Equal(t, 12.0, fired[0].Data["value"])
	require.Len(t, sink.byType(DppsEventAlertResolved), 1)

	// Billing: a balance falling below the watched level, and a key out of quota.
	DppsAfterBillingApply(ctx, &UsageBillingCommand{UserID: 7, APIKeyID: 70, BalanceCost: 3}, &UsageBillingApplyResult{NewBalance: dppsF64(4), APIKeyQuotaExhausted: true})
	DppsAfterBillingApply(ctx, &UsageBillingCommand{UserID: 7}, nil)
	require.Len(t, sink.byType(DppsEventBalanceLow), 1)
	require.Len(t, sink.byType(DppsEventKeyChanged), 3)

	// Seats.
	DppsEmitSeatError(ctx, true, 1, "401")
	DppsEmitSeatError(ctx, false, 1, "401 unauthorized")
	DppsEmitSeatRecovered(ctx, false, 1)
	DppsEmitSeatRecovered(ctx, true, 1)
	require.Len(t, sink.byType(DppsEventSeatStatus), 2, "only transitions")
	DppsEmitSeatClearedIf(ctx, false, 1, "rate_limit")
	DppsEmitSeatClearedIf(ctx, true, 1, "rate_limit")
	require.Len(t, sink.byType(DppsEventSeatCleared), 1)
	DppsEmitSeatsAutoPaused(ctx, []int64{4, 5})
	require.Len(t, sink.byType(DppsEventSeatPaused), 2)
}

// A hook left in place while the sink is off must cost nothing and report nothing.
func TestDppsHookHelpers_NoopSinkReportsNothing(t *testing.T) {
	SetDppsEventSink(nil)
	ctx := context.Background()
	require.NotPanics(t, func() {
		(&adminServiceImpl{}).dppsAfterGroupCreate(ctx, &Group{ID: 1})
		DppsEmitKeyUpdated(ctx, &APIKey{Status: "x"}, "y", true)
		DppsAfterBillingApply(ctx, &UsageBillingCommand{}, &UsageBillingApplyResult{APIKeyQuotaExhausted: true})
	})
	require.Nil(t, DppsEventsInstance())
}
