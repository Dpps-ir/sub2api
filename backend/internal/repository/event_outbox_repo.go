package repository

import (
	"context"
	"database/sql"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/service"
)

type eventOutboxRepository struct{ db *sql.DB }

// NewEventOutboxRepository stores outgoing webhook events in the event_outbox table.
func NewEventOutboxRepository(db *sql.DB) service.EventOutboxRepository {
	return &eventOutboxRepository{db: db}
}

func (r *eventOutboxRepository) Insert(ctx context.Context, rec service.EventOutboxRecord) error {
	_, err := r.db.ExecContext(ctx, `
		INSERT INTO event_outbox (id, event_type, body, status, attempts, created_at, next_attempt_at)
		VALUES ($1, $2, $3::jsonb, 'pending', 0, $4, $5)
		ON CONFLICT (id) DO NOTHING`, rec.ID, rec.Type, string(rec.Body), rec.CreatedAt, rec.NextAttemptAt)
	return err
}

func (r *eventOutboxRepository) ListDue(ctx context.Context, now time.Time, limit int) ([]service.EventOutboxRecord, error) {
	rows, err := r.db.QueryContext(ctx, `
		SELECT id, event_type, body::text, created_at, next_attempt_at, attempts
		FROM event_outbox
		WHERE status = 'pending' AND next_attempt_at <= $1
		ORDER BY created_at ASC
		LIMIT $2`, now, limit)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var out []service.EventOutboxRecord
	for rows.Next() {
		var rec service.EventOutboxRecord
		var body string
		if err := rows.Scan(&rec.ID, &rec.Type, &body, &rec.CreatedAt, &rec.NextAttemptAt, &rec.Attempts); err != nil {
			return nil, err
		}
		rec.Body = []byte(body)
		out = append(out, rec)
	}
	return out, rows.Err()
}

func (r *eventOutboxRepository) MarkSent(ctx context.Context, id string, now time.Time) error {
	_, err := r.db.ExecContext(ctx, `
		UPDATE event_outbox SET status = 'sent', attempts = attempts + 1, last_error = NULL, finished_at = $2
		WHERE id = $1`, id, now)
	return err
}

func (r *eventOutboxRepository) Reschedule(ctx context.Context, id string, attempts int, next time.Time, errClass string) error {
	_, err := r.db.ExecContext(ctx, `
		UPDATE event_outbox SET attempts = $2, next_attempt_at = $3, last_error = $4
		WHERE id = $1 AND status = 'pending'`, id, attempts, next, errClass)
	return err
}

func (r *eventOutboxRepository) MarkFailed(ctx context.Context, id string, attempts int, errClass string, now time.Time) error {
	_, err := r.db.ExecContext(ctx, `
		UPDATE event_outbox SET status = 'failed', attempts = $2, last_error = $3, finished_at = $4
		WHERE id = $1 AND status = 'pending'`, id, attempts, errClass, now)
	return err
}

func (r *eventOutboxRepository) Prune(ctx context.Context, before time.Time) (int64, error) {
	res, err := r.db.ExecContext(ctx, `DELETE FROM event_outbox WHERE status <> 'pending' AND finished_at < $1`, before)
	if err != nil {
		return 0, err
	}
	return res.RowsAffected()
}
