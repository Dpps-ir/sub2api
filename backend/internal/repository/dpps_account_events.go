package repository

import (
	"context"

	dbaccount "github.com/Wei-Shaw/sub2api/ent/account"
	"github.com/Wei-Shaw/sub2api/internal/service"
)

// DPPS hook helpers for account_repo.go (events webhook, SC-28). The upstream file
// carries one-line calls marked "// DPPS hook"; the logic is here. Every read
// below runs only while the events webhook wants the event, so nothing extra
// happens while it is off.

// dppsSeatInError reads the account's status. seat.status is a transition: a repeated
// SetError, or a ClearError on a healthy account, is not one. An unreadable status
// counts as "not in error".
func (r *accountRepository) dppsSeatInError(ctx context.Context, id int64) bool {
	if !service.DppsEventWanted(service.DppsEventSeatStatus) || r.client == nil {
		return false
	}
	acc, err := r.client.Account.Query().Where(dbaccount.IDEQ(id)).Select(dbaccount.FieldStatus).Only(ctx)
	return err == nil && acc != nil && acc.Status == service.StatusError
}

// dppsHoldsRateLimit reports whether the account holds a rate limit or overload.
// Callers clear defensively (the admin "clear error" runs it on every account), and
// an event per such call would be noise, so only a row that held one counts as cleared.
func (r *accountRepository) dppsHoldsRateLimit(ctx context.Context, id int64) bool {
	if !service.DppsEventWanted(service.DppsEventSeatCleared) || r.client == nil {
		return false
	}
	ok, err := r.client.Account.Query().Where(
		dbaccount.IDEQ(id),
		dbaccount.Or(dbaccount.RateLimitedAtNotNil(), dbaccount.RateLimitResetAtNotNil(), dbaccount.OverloadUntilNotNil()),
	).Exist(ctx)
	return err == nil && ok
}

// dppsHoldsTempPause reports whether the account holds a temporary pause (see dppsHoldsRateLimit).
func (r *accountRepository) dppsHoldsTempPause(ctx context.Context, id int64) bool {
	if !service.DppsEventWanted(service.DppsEventSeatCleared) || r.sql == nil {
		return false
	}
	rows, err := r.sql.QueryContext(ctx, `
		SELECT 1 FROM accounts
		WHERE id = $1
			AND deleted_at IS NULL
			AND (temp_unschedulable_until IS NOT NULL OR temp_unschedulable_reason IS NOT NULL)
	`, id)
	if err != nil || rows == nil {
		return false
	}
	defer func() { _ = rows.Close() }()
	return rows.Next()
}
