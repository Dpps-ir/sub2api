//go:build integration

package repository

import (
	"context"
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
