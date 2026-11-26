//go:build integration

package repository

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/service"
)

// A seat in a holder's dpps-plan-* group must leave every non-dpps-* group (D-269): otherwise keys bound to the shared
// group keep being scheduled on a dedicated seat. A pool-only seat keeps its other memberships.
func TestDppsSeatRepo_PlanGroupMakesSeatExclusive(t *testing.T) {
	ctx := context.Background()
	suffix := fmt.Sprintf("%d", time.Now().UnixNano())
	shared := mustCreateGroup(t, integrationEntClient, &service.Group{Name: "openai-default-" + suffix, Platform: service.PlatformOpenAI})
	pool := mustCreateGroup(t, integrationEntClient, &service.Group{Name: "dpps-pool-" + suffix, Platform: service.PlatformOpenAI})
	plan := mustCreateGroup(t, integrationEntClient, &service.Group{Name: "dpps-plan-" + suffix, Platform: service.PlatformOpenAI})
	acc := mustCreateAccount(t, integrationEntClient, &service.Account{Name: "seat-" + suffix, Platform: service.PlatformOpenAI, Type: service.AccountTypeOAuth})
	mustBindAccountToGroup(t, integrationEntClient, acc.ID, shared.ID, 1)
	repo := NewDppsSeatRepository(integrationEntClient, integrationDB, nil)

	memberships := func() []string {
		rows, err := integrationDB.QueryContext(ctx, `SELECT g.name FROM account_groups ag JOIN groups g ON g.id = ag.group_id WHERE ag.account_id = $1`, acc.ID)
		if err != nil {
			t.Fatalf("read memberships: %v", err)
		}
		defer func() { _ = rows.Close() }()
		var out []string
		for rows.Next() {
			var n string
			if err := rows.Scan(&n); err != nil {
				t.Fatalf("scan: %v", err)
			}
			out = append(out, n)
		}
		sort.Strings(out)
		return out
	}

	if _, err := repo.SetAccountDppsGroups(ctx, acc.ID, []string{pool.Name}, nil); err != nil {
		t.Fatalf("pool only: %v", err)
	}
	if got, want := fmt.Sprint(memberships()), fmt.Sprint([]string{pool.Name, shared.Name}); got != want {
		t.Fatalf("pool-only seat must keep its shared group: got %s, want %s", got, want)
	}

	res, err := repo.SetAccountDppsGroups(ctx, acc.ID, []string{plan.Name}, nil)
	if err != nil {
		t.Fatalf("plan group: %v", err)
	}
	if got, want := fmt.Sprint(memberships()), fmt.Sprint([]string{plan.Name}); got != want {
		t.Fatalf("dedicated seat must leave every non-dpps group: got %s, want %s", got, want)
	}
	for _, g := range res.Groups {
		if g.Name == shared.Name {
			t.Fatalf("result still lists the shared group %q", shared.Name)
		}
	}
}

// D-303: the Seat API answers each account's name. The live core answered "" for every seat on 2026-11-26 because
// these queries never selected the column; a fake repository in the service test could not see that.
func TestDppsSeatRepo_SeatsCarryTheAccountName(t *testing.T) {
	ctx := context.Background()
	suffix := fmt.Sprintf("%d", time.Now().UnixNano())
	acc := mustCreateAccount(t, integrationEntClient, &service.Account{Name: "named-seat-" + suffix + "@example.com", Platform: service.PlatformOpenAI, Type: service.AccountTypeOAuth})
	repo := NewDppsSeatRepository(integrationEntClient, integrationDB, nil)

	got, err := repo.GetOpenAIOAuthSeatByID(ctx, acc.ID)
	if err != nil {
		t.Fatalf("get seat: %v", err)
	}
	if got.Name != acc.Name {
		t.Fatalf("get: name %q, want %q", got.Name, acc.Name)
	}
	list, err := repo.ListOpenAIOAuthSeats(ctx)
	if err != nil {
		t.Fatalf("list seats: %v", err)
	}
	for _, s := range list {
		if s.ID == acc.ID {
			if s.Name != acc.Name {
				t.Fatalf("list: name %q, want %q", s.Name, acc.Name)
			}
			return
		}
	}
	t.Fatalf("seat %d not listed", acc.ID)
}

// D-312: the Seat API answers paused_until (the core's auto-pause end, only while in the future) and
// scheduling_threshold (the per-account override). The queries are hand-written SQL, so both fields are proven against
// real Postgres, and no other credential key may come out of them.
func TestDppsSeatRepo_SeatsCarryPausedUntilAndSchedulingThreshold(t *testing.T) {
	ctx := context.Background()
	suffix := fmt.Sprintf("%d", time.Now().UnixNano())
	repo := NewDppsSeatRepository(integrationEntClient, integrationDB, nil)
	svc := service.NewDppsSeatService(repo, nil, nil, nil, nil)

	paused := mustCreateAccount(t, integrationEntClient, &service.Account{
		Name: "paused-" + suffix, Platform: service.PlatformOpenAI, Type: service.AccountTypeOAuth,
		Credentials: map[string]any{"access_token": "SENTINEL_TOKEN", "account_scheduling_threshold": 100},
	})
	plain := mustCreateAccount(t, integrationEntClient, &service.Account{Name: "plain-" + suffix, Platform: service.PlatformOpenAI, Type: service.AccountTypeOAuth})
	expired := mustCreateAccount(t, integrationEntClient, &service.Account{Name: "expired-" + suffix, Platform: service.PlatformOpenAI, Type: service.AccountTypeOAuth})
	until := time.Now().UTC().Add(2 * time.Hour).Truncate(time.Second)
	if _, err := integrationDB.ExecContext(ctx, `UPDATE accounts SET temp_unschedulable_until = $2 WHERE id = $1`, paused.ID, until); err != nil {
		t.Fatalf("seed pause: %v", err)
	}
	if _, err := integrationDB.ExecContext(ctx, `UPDATE accounts SET temp_unschedulable_until = $2 WHERE id = $1`, expired.ID, time.Now().UTC().Add(-time.Hour)); err != nil {
		t.Fatalf("seed expired pause: %v", err)
	}

	check := func(label string, s service.DppsSeat) {
		switch s.ID {
		case paused.ID:
			if s.PausedUntil == nil || !s.PausedUntil.Equal(until) {
				t.Fatalf("%s: paused_until %v, want %v", label, s.PausedUntil, until)
			}
			if s.SchedulingThreshold == nil || *s.SchedulingThreshold != 100 {
				t.Fatalf("%s: scheduling_threshold %v, want 100", label, s.SchedulingThreshold)
			}
		case plain.ID:
			if s.PausedUntil != nil || s.SchedulingThreshold != nil {
				t.Fatalf("%s: plain seat must answer null/null, got %v/%v", label, s.PausedUntil, s.SchedulingThreshold)
			}
		case expired.ID:
			if s.PausedUntil != nil {
				t.Fatalf("%s: a pause in the past must answer null, got %v", label, s.PausedUntil)
			}
		}
	}
	list, err := svc.ListSeats(ctx)
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	seen := 0
	for _, s := range list {
		check("list", s)
		if s.ID == paused.ID || s.ID == plain.ID || s.ID == expired.ID {
			seen++
		}
	}
	if seen != 3 {
		t.Fatalf("list returned %d of the 3 seeded seats", seen)
	}
	for _, id := range []int64{paused.ID, plain.ID, expired.ID} {
		s, err := svc.GetSeat(ctx, id)
		if err != nil {
			t.Fatalf("get %d: %v", id, err)
		}
		check("get", *s)
	}
	// Account rows the SELECT returns carry only the two allowed credential keys.
	acc, err := repo.GetOpenAIOAuthSeatByID(ctx, paused.ID)
	if err != nil {
		t.Fatalf("get account: %v", err)
	}
	if _, leaked := acc.Credentials["access_token"]; leaked {
		t.Fatalf("access_token left the seat SELECT")
	}
}

// D-312: the route's repository call sets or removes only credentials.account_scheduling_threshold. Secret keys seeded
// beside it must be identical afterwards; a non-OpenAI-OAuth account is "not found"; a repeat changes nothing.
func TestDppsSeatRepo_SetSchedulingThresholdTouchesOnlyItsKey(t *testing.T) {
	ctx := context.Background()
	suffix := fmt.Sprintf("%d", time.Now().UnixNano())
	repo := NewDppsSeatRepository(integrationEntClient, integrationDB, nil)
	secrets := map[string]any{"access_token": "SENTINEL_ACCESS", "refresh_token": "SENTINEL_REFRESH", "id_token": "SENTINEL_ID", "subscription_expires_at": "2026-12-20T09:00:00Z"}
	acc := mustCreateAccount(t, integrationEntClient, &service.Account{Name: "thr-" + suffix, Platform: service.PlatformOpenAI, Type: service.AccountTypeOAuth, Credentials: secrets})
	other := mustCreateAccount(t, integrationEntClient, &service.Account{Name: "api-" + suffix, Platform: service.PlatformOpenAI, Type: service.AccountTypeAPIKey})

	creds := func(id int64) map[string]any {
		var raw string
		if err := integrationDB.QueryRowContext(ctx, `SELECT credentials::text FROM accounts WHERE id = $1`, id).Scan(&raw); err != nil {
			t.Fatalf("read credentials: %v", err)
		}
		var m map[string]any
		if err := json.Unmarshal([]byte(raw), &m); err != nil {
			t.Fatalf("decode credentials: %v", err)
		}
		return m
	}
	assertSecrets := func(label string, m map[string]any) {
		for k, v := range secrets {
			if m[k] != v {
				t.Fatalf("%s: credential %q changed: got %v, want %v", label, k, m[k], v)
			}
		}
	}
	outbox := func() int {
		var n int
		if err := integrationDB.QueryRowContext(ctx, `SELECT COUNT(*) FROM scheduler_outbox WHERE account_id = $1 AND event_type = $2`, acc.ID, service.SchedulerOutboxEventAccountChanged).Scan(&n); err != nil {
			t.Fatalf("count outbox: %v", err)
		}
		return n
	}
	t.Cleanup(func() {
		_, _ = integrationDB.ExecContext(ctx, `DELETE FROM scheduler_outbox WHERE account_id = $1`, acc.ID)
	})

	v := 100
	if err := repo.SetAccountSchedulingThreshold(ctx, acc.ID, &v); err != nil {
		t.Fatalf("set: %v", err)
	}
	m := creds(acc.ID)
	if got, _ := m["account_scheduling_threshold"].(float64); got != 100 {
		t.Fatalf("threshold %v, want 100", m["account_scheduling_threshold"])
	}
	assertSecrets("after set", m)
	if outbox() == 0 {
		t.Fatalf("no scheduler outbox event after set")
	}
	if err := repo.SetAccountSchedulingThreshold(ctx, acc.ID, &v); err != nil {
		t.Fatalf("repeat set: %v", err)
	}
	if got, _ := creds(acc.ID)["account_scheduling_threshold"].(float64); got != 100 {
		t.Fatalf("repeat changed the value to %v", got)
	}
	if err := repo.SetAccountSchedulingThreshold(ctx, acc.ID, nil); err != nil {
		t.Fatalf("clear: %v", err)
	}
	m = creds(acc.ID)
	if _, has := m["account_scheduling_threshold"]; has {
		t.Fatalf("clear left the key: %v", m)
	}
	assertSecrets("after clear", m)
	if err := repo.SetAccountSchedulingThreshold(ctx, acc.ID, nil); err != nil {
		t.Fatalf("repeat clear: %v", err)
	}
	if err := repo.SetAccountSchedulingThreshold(ctx, other.ID, &v); !errors.Is(err, service.ErrDppsSeatNotFound) {
		t.Fatalf("API-key account: got %v, want ErrDppsSeatNotFound", err)
	}
	if err := repo.SetAccountSchedulingThreshold(ctx, 0x7fffffff, &v); !errors.Is(err, service.ErrDppsSeatNotFound) {
		t.Fatalf("missing account: got %v, want ErrDppsSeatNotFound", err)
	}
}
