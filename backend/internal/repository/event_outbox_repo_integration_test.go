//go:build integration

package repository

import (
	"context"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/stretchr/testify/require"
)

// Runs the outbox SQL and migration 253 against a real Postgres.
func TestEventOutboxRepository_Lifecycle(t *testing.T) {
	ctx := context.Background()
	repo := NewEventOutboxRepository(integrationDB)
	prefix := "it-" + time.Now().Format("150405.000000") + "-"
	t.Cleanup(func() { _, _ = integrationDB.ExecContext(ctx, `DELETE FROM event_outbox WHERE id LIKE $1`, prefix+"%") })

	now := time.Now().UTC().Truncate(time.Millisecond)
	rec := func(n string, created time.Time, next time.Time) service.EventOutboxRecord {
		return service.EventOutboxRecord{ID: prefix + n, Type: "seat.status", Body: []byte(`{"id":"` + prefix + n + `","type":"seat.status","data":{"k":1}}`), CreatedAt: created, NextAttemptAt: next}
	}
	require.NoError(t, repo.Insert(ctx, rec("a", now.Add(-2*time.Minute), now.Add(-time.Minute))))
	require.NoError(t, repo.Insert(ctx, rec("a", now, now)), "a repeated id is ignored")
	require.NoError(t, repo.Insert(ctx, rec("b", now.Add(-time.Minute), now.Add(-time.Second))))
	require.NoError(t, repo.Insert(ctx, rec("later", now, now.Add(time.Hour))))

	due, err := repo.ListDue(ctx, now, 100)
	require.NoError(t, err)
	var ids []string
	for _, d := range due {
		if len(d.ID) > len(prefix) && d.ID[:len(prefix)] == prefix {
			ids = append(ids, d.ID)
		}
	}
	require.Equal(t, []string{prefix + "a", prefix + "b"}, ids, "due rows only, oldest first")
	require.JSONEq(t, `{"id":"`+prefix+`a","type":"seat.status","data":{"k":1}}`, string(due[0].Body))

	require.NoError(t, repo.Reschedule(ctx, prefix+"a", 1, now.Add(10*time.Second), "http_500"))
	require.NoError(t, repo.MarkSent(ctx, prefix+"b", now))
	due, err = repo.ListDue(ctx, now, 100)
	require.NoError(t, err)
	for _, d := range due {
		require.NotEqual(t, prefix+"a", d.ID, "rescheduled: not due yet")
		require.NotEqual(t, prefix+"b", d.ID, "sent: never again")
	}

	require.NoError(t, repo.MarkFailed(ctx, prefix+"a", 9, "deadline", now))
	var status, lastErr string
	require.NoError(t, integrationDB.QueryRowContext(ctx, `SELECT status, last_error FROM event_outbox WHERE id = $1`, prefix+"a").Scan(&status, &lastErr))
	require.Equal(t, "failed", status)
	require.Equal(t, "deadline", lastErr)

	n, err := repo.Prune(ctx, now.Add(time.Minute))
	require.NoError(t, err)
	require.GreaterOrEqual(t, n, int64(2), "sent and failed rows older than the cut are pruned")
	var left int
	require.NoError(t, integrationDB.QueryRowContext(ctx, `SELECT count(*) FROM event_outbox WHERE id = $1`, prefix+"later").Scan(&left))
	require.Equal(t, 1, left, "a pending row is never pruned")
}
