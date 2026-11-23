package repository

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"sort"
	"strings"
	"time"

	dbent "github.com/Wei-Shaw/sub2api/ent"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/lib/pq"
)

type dppsSeatRepository struct {
	client         *dbent.Client
	sql            sqlExecutor
	schedulerCache service.SchedulerCache
}

// NewDppsSeatRepository constructs the PostgreSQL repository backing DppsSeatService.
func NewDppsSeatRepository(client *dbent.Client, db *sql.DB, schedulerCache service.SchedulerCache) service.DppsSeatRepository {
	var exec sqlExecutor
	if db != nil {
		exec = db
	} else if client != nil {
		exec = client
	}
	return newDppsSeatRepositoryWithSQL(client, exec, schedulerCache)
}

func newDppsSeatRepositoryWithSQL(client *dbent.Client, sqlq sqlExecutor, schedulerCache service.SchedulerCache) *dppsSeatRepository {
	return &dppsSeatRepository{
		client:         client,
		sql:            sqlq,
		schedulerCache: schedulerCache,
	}
}

func (r *dppsSeatRepository) executor(ctx context.Context) sqlExecutor {
	if tx := dbent.TxFromContext(ctx); tx != nil {
		return tx.Client()
	}
	if r == nil {
		return nil
	}
	return r.sql
}

func (r *dppsSeatRepository) ListOpenAIOAuthSeats(ctx context.Context) ([]service.Account, error) {
	exec := r.executor(ctx)
	if exec == nil {
		return nil, errors.New("dpps seat repository is not initialized")
	}

	rows, err := exec.QueryContext(ctx, `
		SELECT
			id,
			platform,
			type,
			status,
			schedulable,
			COALESCE(extra, '{}'::jsonb)::text,
			created_at,
			COALESCE(error_message, ''),
			COALESCE((credentials -> 'subscription_expires_at')::text, '')
		FROM accounts
		WHERE platform = $1
		  AND type = $2
		  AND deleted_at IS NULL
		ORDER BY id ASC
	`, service.PlatformOpenAI, service.AccountTypeOAuth)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()

	accounts := make([]service.Account, 0)
	accountIDs := make([]int64, 0)
	for rows.Next() {
		acc, scanErr := dppsScanSeatAccountRow(rows)
		if scanErr != nil {
			return nil, scanErr
		}
		accounts = append(accounts, acc)
		accountIDs = append(accountIDs, acc.ID)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	if err := dppsAttachSeatGroups(ctx, exec, accounts, accountIDs); err != nil {
		return nil, err
	}
	return accounts, nil
}

func (r *dppsSeatRepository) GetOpenAIOAuthSeatByID(ctx context.Context, accountID int64) (*service.Account, error) {
	exec := r.executor(ctx)
	if exec == nil {
		return nil, errors.New("dpps seat repository is not initialized")
	}

	rows, err := exec.QueryContext(ctx, `
		SELECT
			id,
			platform,
			type,
			status,
			schedulable,
			COALESCE(extra, '{}'::jsonb)::text,
			created_at,
			COALESCE(error_message, ''),
			COALESCE((credentials -> 'subscription_expires_at')::text, '')
		FROM accounts
		WHERE id = $1
		  AND platform = $2
		  AND type = $3
		  AND deleted_at IS NULL
	`, accountID, service.PlatformOpenAI, service.AccountTypeOAuth)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()

	if !rows.Next() {
		if err := rows.Err(); err != nil {
			return nil, err
		}
		return nil, service.ErrDppsSeatNotFound
	}
	acc, err := dppsScanSeatAccountRow(rows)
	if err != nil {
		return nil, err
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	accounts := []service.Account{acc}
	if err := dppsAttachSeatGroups(ctx, exec, accounts, []int64{acc.ID}); err != nil {
		return nil, err
	}
	return &accounts[0], nil
}

func (r *dppsSeatRepository) GetActiveGroupByName(ctx context.Context, name string) (*service.Group, error) {
	exec := r.executor(ctx)
	if exec == nil {
		return nil, errors.New("dpps seat repository is not initialized")
	}

	var (
		g                      service.Group
		fallbackGroupID        sql.NullInt64
		fallbackOnInvalidReqID sql.NullInt64
		dailyLimit             sql.NullFloat64
		weeklyLimit            sql.NullFloat64
		monthlyLimit           sql.NullFloat64
	)
	err := scanSingleRow(ctx, exec, `
		SELECT
			id,
			name,
			platform,
			is_exclusive,
			subscription_type,
			fallback_group_id,
			fallback_group_id_on_invalid_request,
			claude_code_only,
			status,
			allow_messages_dispatch,
			rate_multiplier,
			require_oauth_only,
			require_privacy_set,
			daily_limit_usd,
			weekly_limit_usd,
			monthly_limit_usd,
			created_at,
			updated_at
		FROM groups
		WHERE name = $1
		  AND deleted_at IS NULL
	`, []any{name},
		&g.ID,
		&g.Name,
		&g.Platform,
		&g.IsExclusive,
		&g.SubscriptionType,
		&fallbackGroupID,
		&fallbackOnInvalidReqID,
		&g.ClaudeCodeOnly,
		&g.Status,
		&g.AllowMessagesDispatch,
		&g.RateMultiplier,
		&g.RequireOAuthOnly,
		&g.RequirePrivacySet,
		&dailyLimit,
		&weeklyLimit,
		&monthlyLimit,
		&g.CreatedAt,
		&g.UpdatedAt,
	)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, service.ErrGroupNotFound
		}
		return nil, err
	}

	g.Hydrated = true
	if fallbackGroupID.Valid {
		v := fallbackGroupID.Int64
		g.FallbackGroupID = &v
	}
	if fallbackOnInvalidReqID.Valid {
		v := fallbackOnInvalidReqID.Int64
		g.FallbackGroupIDOnInvalidRequest = &v
	}
	if dailyLimit.Valid {
		v := dailyLimit.Float64
		g.DailyLimitUSD = &v
	}
	if weeklyLimit.Valid {
		v := weeklyLimit.Float64
		g.WeeklyLimitUSD = &v
	}
	if monthlyLimit.Valid {
		v := monthlyLimit.Float64
		g.MonthlyLimitUSD = &v
	}
	return &g, nil
}

func (r *dppsSeatRepository) SetAccountDppsGroups(
	ctx context.Context,
	accountID int64,
	groupNames []string,
	validateGroup func(*service.Group) error,
) (*service.DppsSetSeatGroupsResult, error) {
	if r == nil {
		return nil, errors.New("dpps seat repository is not initialized")
	}

	var (
		tx   *dbent.Tx
		exec sqlExecutor
	)
	if r.client != nil {
		var txErr error
		tx, txErr = r.client.Tx(ctx)
		if txErr != nil && !errors.Is(txErr, dbent.ErrTxStarted) {
			return nil, txErr
		}
		if txErr == nil {
			defer func() { _ = tx.Rollback() }()
			exec = tx.Client()
		} else {
			exec = r.client
		}
	} else {
		exec = r.executor(ctx)
	}
	if exec == nil {
		return nil, errors.New("dpps seat repository is not initialized")
	}

	// Lock the target account row and verify OpenAI OAuth eligibility.
	var platform, accType string
	err := scanSingleRow(ctx, exec, `
		SELECT platform, type
		FROM accounts
		WHERE id = $1 AND deleted_at IS NULL
		FOR SHARE
	`, []any{accountID}, &platform, &accType)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, service.ErrDppsSeatNotFound
		}
		return nil, err
	}
	if platform != service.PlatformOpenAI || accType != service.AccountTypeOAuth {
		return nil, service.ErrDppsSeatNotFound
	}

	// Lock and validate requested dpps-* groups inside the same transaction.
	targetGroups := make([]service.Group, 0, len(groupNames))
	if len(groupNames) > 0 {
		byName, fetchErr := dppsLockAndLoadGroupsByName(ctx, exec, groupNames)
		if fetchErr != nil {
			return nil, fetchErr
		}
		for _, name := range groupNames {
			g, ok := byName[name]
			if !ok {
				return nil, service.ErrGroupNotFound
			}
			if validateGroup != nil {
				if valErr := validateGroup(&g); valErr != nil {
					return nil, valErr
				}
			}
			targetGroups = append(targetGroups, g)
		}
	}

	// Load existing active memberships ordered by priority ASC, group_id ASC.
	existingRows, err := exec.QueryContext(ctx, `
		SELECT ag.group_id, g.name
		FROM account_groups ag
		JOIN groups g ON g.id = ag.group_id
		WHERE ag.account_id = $1 AND g.deleted_at IS NULL
		ORDER BY ag.priority ASC, ag.group_id ASC
	`, accountID)
	if err != nil {
		return nil, err
	}

	oldGroupIDs := make([]int64, 0)
	nonDppsGroups := make([]service.Group, 0)
	for existingRows.Next() {
		var gid int64
		var gname string
		if scanErr := existingRows.Scan(&gid, &gname); scanErr != nil {
			_ = existingRows.Close()
			return nil, scanErr
		}
		oldGroupIDs = append(oldGroupIDs, gid)
		if !strings.HasPrefix(gname, "dpps-") {
			nonDppsGroups = append(nonDppsGroups, service.Group{ID: gid, Name: gname})
		}
	}
	if closeErr := existingRows.Close(); closeErr != nil {
		return nil, closeErr
	}
	if rowErr := existingRows.Err(); rowErr != nil {
		return nil, rowErr
	}

	// A seat placed in a holder's dpps-plan-* group is dedicated to that holder or shared only with one other half-seat
	// holder (D-269, D-270): it leaves every non-dpps-* group, or keys bound to those groups (for example the wallet keys
	// still on openai-default before the cutover) would keep being scheduled on it. A pool-only seat keeps them.
	if dppsGroupNamesIncludePlan(groupNames) {
		nonDppsGroups = nonDppsGroups[:0]
	}

	// Preserve non-dpps-* memberships (pool-only seats) and replace dpps-* subset deterministically.
	finalGroups := make([]service.Group, 0, len(nonDppsGroups)+len(targetGroups))
	finalGroupIDs := make([]int64, 0, len(nonDppsGroups)+len(targetGroups))
	seenIDs := make(map[int64]struct{}, len(nonDppsGroups)+len(targetGroups))
	for _, g := range nonDppsGroups {
		if _, ok := seenIDs[g.ID]; ok {
			continue
		}
		seenIDs[g.ID] = struct{}{}
		finalGroups = append(finalGroups, g)
		finalGroupIDs = append(finalGroupIDs, g.ID)
	}
	for _, g := range targetGroups {
		if _, ok := seenIDs[g.ID]; ok {
			continue
		}
		seenIDs[g.ID] = struct{}{}
		finalGroups = append(finalGroups, g)
		finalGroupIDs = append(finalGroupIDs, g.ID)
	}

	if _, err := exec.ExecContext(ctx, `DELETE FROM account_groups WHERE account_id = $1`, accountID); err != nil {
		return nil, err
	}
	if len(finalGroupIDs) > 0 {
		if _, err := exec.ExecContext(ctx, `
			INSERT INTO account_groups (account_id, group_id, priority, created_at)
			SELECT $1, x.group_id, x.ord::int, NOW()
			FROM unnest($2::bigint[]) WITH ORDINALITY AS x(group_id, ord)
		`, accountID, pq.Array(finalGroupIDs)); err != nil {
			return nil, err
		}
	}

	// Unconditionally enqueue outbox event even when finalGroupIDs is empty so scheduler
	// buckets drop retired or ungrouped seats instead of retaining stale Redis entries.
	affectedGroupIDs := mergeGroupIDs(oldGroupIDs, finalGroupIDs)
	if affectedGroupIDs == nil {
		affectedGroupIDs = []int64{}
	}
	outboxPayload := map[string]any{"group_ids": affectedGroupIDs}
	if err := enqueueSchedulerOutbox(ctx, exec, service.SchedulerOutboxEventAccountGroupsChanged, &accountID, nil, outboxPayload); err != nil {
		return nil, err
	}

	if tx != nil {
		if err := tx.Commit(); err != nil {
			return nil, err
		}
	}

	if r.client != nil && r.schedulerCache != nil {
		newAccountRepositoryWithSQL(r.client, r.sql, r.schedulerCache).syncSchedulerAccountSnapshot(ctx, accountID)
	}

	groups := make([]service.DppsSeatGroupRef, 0, len(finalGroups))
	dppsGroups := make([]string, 0, len(targetGroups))
	for _, g := range finalGroups {
		groups = append(groups, service.DppsSeatGroupRef{ID: g.ID, Name: g.Name})
		if strings.HasPrefix(g.Name, "dpps-") {
			dppsGroups = append(dppsGroups, g.Name)
		}
	}
	sort.Strings(dppsGroups)

	return &service.DppsSetSeatGroupsResult{
		ID:         accountID,
		DppsGroups: dppsGroups,
		Groups:     groups,
	}, nil
}

func (r *dppsSeatRepository) GetUsageWindow(ctx context.Context, accountID int64, since, until time.Time) ([]service.DppsUsageItem, error) {
	exec := r.executor(ctx)
	if exec == nil {
		return nil, errors.New("dpps seat repository is not initialized")
	}

	rows, err := exec.QueryContext(ctx, `
		SELECT
			account_id,
			api_key_id,
			COUNT(*)::bigint AS requests,
			COALESCE(SUM(input_tokens), 0)::bigint AS input_tokens,
			COALESCE(SUM(output_tokens), 0)::bigint AS output_tokens,
			COALESCE(SUM(cache_creation_tokens), 0)::bigint AS cache_creation_tokens,
			COALESCE(SUM(cache_read_tokens), 0)::bigint AS cache_read_tokens,
			COALESCE(SUM(total_cost), 0)::double precision AS total_cost,
			COALESCE(SUM(actual_cost), 0)::double precision AS actual_cost,
			COALESCE(SUM(account_stats_cost), 0)::double precision AS account_stats_cost
		FROM usage_logs
		WHERE ($1::bigint = 0 OR account_id = $1)
		  AND created_at >= $2
		  AND created_at < $3
		GROUP BY account_id, api_key_id
		ORDER BY account_id ASC, api_key_id ASC
	`, accountID, since.UTC(), until.UTC())
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()

	items := make([]service.DppsUsageItem, 0)
	for rows.Next() {
		var item service.DppsUsageItem
		if scanErr := rows.Scan(
			&item.AccountID,
			&item.APIKeyID,
			&item.Requests,
			&item.InputTokens,
			&item.OutputTokens,
			&item.CacheCreationTokens,
			&item.CacheReadTokens,
			&item.TotalCost,
			&item.ActualCost,
			&item.AccountStatsCost,
		); scanErr != nil {
			return nil, scanErr
		}
		items = append(items, item)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return items, nil
}

func dppsScanSeatAccountRow(rows *sql.Rows) (service.Account, error) {
	var (
		acc       service.Account
		extraRaw  string
		subExpRaw string
	)
	if err := rows.Scan(
		&acc.ID,
		&acc.Platform,
		&acc.Type,
		&acc.Status,
		&acc.Schedulable,
		&extraRaw,
		&acc.CreatedAt,
		&acc.ErrorMessage,
		&subExpRaw,
	); err != nil {
		return service.Account{}, err
	}

	extraTrimmed := strings.TrimSpace(extraRaw)
	if extraTrimmed != "" && extraTrimmed != "null" && extraTrimmed != "{}" {
		dec := json.NewDecoder(strings.NewReader(extraTrimmed))
		dec.UseNumber()
		var extra map[string]any
		if err := dec.Decode(&extra); err != nil {
			return service.Account{}, err
		}
		acc.Extra = extra
	}

	subExpTrimmed := strings.TrimSpace(subExpRaw)
	if subExpTrimmed != "" && subExpTrimmed != "null" {
		dec := json.NewDecoder(strings.NewReader(subExpTrimmed))
		dec.UseNumber()
		var subExpVal any
		if err := dec.Decode(&subExpVal); err == nil && subExpVal != nil {
			acc.Credentials = map[string]any{"subscription_expires_at": subExpVal}
		}
	}

	return acc, nil
}

func dppsAttachSeatGroups(ctx context.Context, exec sqlExecutor, accounts []service.Account, accountIDs []int64) error {
	if len(accounts) == 0 || len(accountIDs) == 0 {
		return nil
	}

	rows, err := exec.QueryContext(ctx, `
		SELECT ag.account_id, ag.group_id, ag.priority, g.name
		FROM account_groups ag
		JOIN groups g ON g.id = ag.group_id
		WHERE ag.account_id = ANY($1)
		  AND g.deleted_at IS NULL
		ORDER BY ag.account_id ASC, ag.priority ASC, ag.group_id ASC
	`, pq.Array(accountIDs))
	if err != nil {
		return err
	}
	defer func() { _ = rows.Close() }()

	groupsByAcc := make(map[int64][]*service.Group, len(accountIDs))
	groupIDsByAcc := make(map[int64][]int64, len(accountIDs))
	agsByAcc := make(map[int64][]service.AccountGroup, len(accountIDs))

	for rows.Next() {
		var (
			accID    int64
			groupID  int64
			priority int
			name     string
		)
		if scanErr := rows.Scan(&accID, &groupID, &priority, &name); scanErr != nil {
			return scanErr
		}
		g := &service.Group{ID: groupID, Name: name}
		groupsByAcc[accID] = append(groupsByAcc[accID], g)
		groupIDsByAcc[accID] = append(groupIDsByAcc[accID], groupID)
		agsByAcc[accID] = append(agsByAcc[accID], service.AccountGroup{
			AccountID: accID,
			GroupID:   groupID,
			Priority:  priority,
			Group:     g,
		})
	}
	if err := rows.Err(); err != nil {
		return err
	}

	for i := range accounts {
		id := accounts[i].ID
		accounts[i].Groups = groupsByAcc[id]
		accounts[i].GroupIDs = groupIDsByAcc[id]
		accounts[i].AccountGroups = agsByAcc[id]
	}
	return nil
}

func dppsLockAndLoadGroupsByName(ctx context.Context, exec sqlExecutor, names []string) (map[string]service.Group, error) {
	rows, err := exec.QueryContext(ctx, `
		SELECT
			id,
			name,
			platform,
			is_exclusive,
			subscription_type,
			fallback_group_id,
			fallback_group_id_on_invalid_request,
			claude_code_only,
			status,
			allow_messages_dispatch
		FROM groups
		WHERE name = ANY($1) AND deleted_at IS NULL
		ORDER BY id ASC
		FOR SHARE
	`, pq.Array(names))
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()

	out := make(map[string]service.Group, len(names))
	for rows.Next() {
		var (
			g                      service.Group
			fallbackGroupID        sql.NullInt64
			fallbackOnInvalidReqID sql.NullInt64
		)
		if scanErr := rows.Scan(
			&g.ID,
			&g.Name,
			&g.Platform,
			&g.IsExclusive,
			&g.SubscriptionType,
			&fallbackGroupID,
			&fallbackOnInvalidReqID,
			&g.ClaudeCodeOnly,
			&g.Status,
			&g.AllowMessagesDispatch,
		); scanErr != nil {
			return nil, scanErr
		}
		if fallbackGroupID.Valid {
			v := fallbackGroupID.Int64
			g.FallbackGroupID = &v
		}
		if fallbackOnInvalidReqID.Valid {
			v := fallbackOnInvalidReqID.Int64
			g.FallbackGroupIDOnInvalidRequest = &v
		}
		g.Hydrated = true
		out[g.Name] = g
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

// dppsGroupNamesIncludePlan reports whether the requested dpps-* groups name a holder's plan group.
func dppsGroupNamesIncludePlan(names []string) bool {
	for _, n := range names {
		if strings.HasPrefix(n, "dpps-plan-") {
			return true
		}
	}
	return false
}
