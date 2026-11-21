package repository

import (
	"context"
	"database/sql"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/service"
)

type dppsEventOutboxRepository struct{ db *sql.DB }

// NewDppsEventOutboxRepository stores outgoing webhook events in the dpps_event_outbox table.
func NewDppsEventOutboxRepository(db *sql.DB) service.DppsEventOutboxRepository {
	return &dppsEventOutboxRepository{db: db}
}

func (r *dppsEventOutboxRepository) Insert(ctx context.Context, rec service.DppsEventOutboxRecord) error {
	_, err := r.db.ExecContext(ctx, `
		INSERT INTO dpps_event_outbox (id, event_type, body, status, attempts, created_at, next_attempt_at)
		VALUES ($1, $2, $3::jsonb, 'pending', 0, $4, $5)
		ON CONFLICT (id) DO NOTHING`, rec.ID, rec.Type, string(rec.Body), rec.CreatedAt, rec.NextAttemptAt)
	return err
}

func (r *dppsEventOutboxRepository) ListDue(ctx context.Context, now time.Time, limit int) ([]service.DppsEventOutboxRecord, error) {
	rows, err := r.db.QueryContext(ctx, `
		SELECT id, event_type, body::text, created_at, next_attempt_at, attempts
		FROM dpps_event_outbox
		WHERE status = 'pending' AND next_attempt_at <= $1
		ORDER BY created_at ASC
		LIMIT $2`, now, limit)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var out []service.DppsEventOutboxRecord
	for rows.Next() {
		var rec service.DppsEventOutboxRecord
		var body string
		if err := rows.Scan(&rec.ID, &rec.Type, &body, &rec.CreatedAt, &rec.NextAttemptAt, &rec.Attempts); err != nil {
			return nil, err
		}
		rec.Body = []byte(body)
		out = append(out, rec)
	}
	return out, rows.Err()
}

func (r *dppsEventOutboxRepository) MarkSent(ctx context.Context, id string, now time.Time) error {
	_, err := r.db.ExecContext(ctx, `
		UPDATE dpps_event_outbox SET status = 'sent', attempts = attempts + 1, last_error = NULL, finished_at = $2
		WHERE id = $1`, id, now)
	return err
}

func (r *dppsEventOutboxRepository) Reschedule(ctx context.Context, id string, attempts int, next time.Time, errClass string) error {
	_, err := r.db.ExecContext(ctx, `
		UPDATE dpps_event_outbox SET attempts = $2, next_attempt_at = $3, last_error = $4
		WHERE id = $1 AND status = 'pending'`, id, attempts, next, errClass)
	return err
}

func (r *dppsEventOutboxRepository) MarkFailed(ctx context.Context, id string, attempts int, errClass string, now time.Time) error {
	_, err := r.db.ExecContext(ctx, `
		UPDATE dpps_event_outbox SET status = 'failed', attempts = $2, last_error = $3, finished_at = $4
		WHERE id = $1 AND status = 'pending'`, id, attempts, errClass, now)
	return err
}

func (r *dppsEventOutboxRepository) Prune(ctx context.Context, before time.Time) (int64, error) {
	res, err := r.db.ExecContext(ctx, `DELETE FROM dpps_event_outbox WHERE status <> 'pending' AND finished_at < $1`, before)
	if err != nil {
		return 0, err
	}
	return res.RowsAffected()
}
