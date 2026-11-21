package repository

import (
	"context"
	"database/sql"
	"sync"
	"testing"
	"time"

	"entgo.io/ent/dialect"
	entsql "entgo.io/ent/dialect/sql"
	sqlmock "github.com/DATA-DOG/go-sqlmock"
	dbent "github.com/Wei-Shaw/sub2api/ent"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/stretchr/testify/require"
)

// repoEventSink records what the account repository hands to the events webhook.
type repoEventSink struct {
	mu     sync.Mutex
	events []repoEvent
}

type repoEvent struct {
	Type string
	Data map[string]any
}

func (s *repoEventSink) Wants(string) bool { return true }
func (s *repoEventSink) Emit(_ context.Context, t string, d map[string]any) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.events = append(s.events, repoEvent{t, d})
}

func (s *repoEventSink) types() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := []string{}
	for _, e := range s.events {
		out = append(out, e.Type)
	}
	return out
}

func installRepoEventSink(t *testing.T) *repoEventSink {
	t.Helper()
	sink := &repoEventSink{}
	service.SetEventSink(sink)
	t.Cleanup(func() { service.SetEventSink(nil) })
	return sink
}

// newEntAccountRepoWithMock builds an account repository whose ent client and raw
// SQL executor share one sqlmock connection. Every write is accepted.
func newEntAccountRepoWithMock(t *testing.T) (*accountRepository, sqlmock.Sqlmock) {
	t.Helper()
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	mock.MatchExpectationsInOrder(false)
	client := dbent.NewClient(dbent.Driver(entsql.OpenDB(dialect.Postgres, db)))
	t.Cleanup(func() { _ = client.Close() })
	return newAccountRepositoryWithSQL(client, db, nil), mock
}

func expectStatusRead(mock sqlmock.Sqlmock, status string) {
	mock.ExpectQuery(`(?s)SELECT .*"status".* FROM "accounts"`).WillReturnRows(sqlmock.NewRows([]string{"status"}).AddRow(status))
}

func expectAccountWrite(mock sqlmock.Sqlmock) {
	mock.ExpectExec(`(?s)UPDATE "accounts"`).WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec(`(?s)INSERT INTO scheduler_outbox`).WillReturnResult(sqlmock.NewResult(1, 1))
}

func TestAccountRepository_EntWritesEmitSeatEventsOnce(t *testing.T) {
	until := time.Date(2026, 11, 20, 15, 0, 0, 0, time.UTC)
	cases := []struct {
		name string
		run  func(*accountRepository) error
		want string
		data map[string]any
	}{
		{"set error", func(r *accountRepository) error {
			return r.SetError(context.Background(), 5, "HTTP 401 unauthorized, token abc")
		},
			service.EventSeatStatus, map[string]any{"status": service.StatusError, "reason": "auth"}},
		{"clear error", func(r *accountRepository) error { return r.ClearError(context.Background(), 5) },
			service.EventSeatStatus, map[string]any{"status": service.StatusActive}},
		{"rate limited", func(r *accountRepository) error { return r.SetRateLimited(context.Background(), 5, until) },
			service.EventSeatLimited, map[string]any{"kind": "rate_limit", "until": "2026-11-20T15:00:00Z"}},
		{"overloaded", func(r *accountRepository) error { return r.SetOverloaded(context.Background(), 5, until) },
			service.EventSeatLimited, map[string]any{"kind": "overload", "until": "2026-11-20T15:00:00Z"}},
		{"clear rate limit", func(r *accountRepository) error { return r.ClearRateLimit(context.Background(), 5) },
			service.EventSeatCleared, map[string]any{"kind": "rate_limit"}},
		{"paused by admin", func(r *accountRepository) error { return r.SetSchedulable(context.Background(), 5, false) },
			service.EventSeatPaused, map[string]any{"by": "admin"}},
		{"resumed by admin", func(r *accountRepository) error { return r.SetSchedulable(context.Background(), 5, true) },
			service.EventSeatResumed, map[string]any{"by": "admin"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			sink := installRepoEventSink(t)
			repo, mock := newEntAccountRepoWithMock(t)
			switch tc.name {
			case "set error":
				expectStatusRead(mock, service.StatusActive)
			case "clear error":
				expectStatusRead(mock, service.StatusError)
			}
			expectAccountWrite(mock)

			require.NoError(t, tc.run(repo))

			require.Equal(t, []string{tc.want}, sink.types(), "exactly one event of the right type")
			got := sink.events[0].Data
			require.EqualValues(t, 5, got["account_id"])
			for k, v := range tc.data {
				require.Equal(t, v, got[k], k)
			}
			require.NotContains(t, got, "message", "raw error text never rides along")
			require.NotContains(t, got, "error_message")
		})
	}
}

func TestAccountRepository_FailedWriteEmitsNothing(t *testing.T) {
	sink := installRepoEventSink(t)
	repo, mock := newEntAccountRepoWithMock(t)
	mock.ExpectExec(`(?s)UPDATE "accounts"`).WillReturnError(sql.ErrConnDone)

	require.Error(t, repo.SetError(context.Background(), 5, "boom"))
	require.Empty(t, sink.types())
}

func TestAccountRepository_TempUnschedulableAndClearEmitOnce(t *testing.T) {
	sink := installRepoEventSink(t)
	until := time.Date(2026, 11, 20, 15, 0, 0, 0, time.UTC)

	none := &recordingSQLExecutor{result: rowsAffectedResult(0)}
	require.NoError(t, newAccountRepositoryWithSQL(nil, none, nil).SetTempUnschedulable(context.Background(), 5, until, "retry"))
	require.Empty(t, sink.types(), "nothing changed: no event")

	hit := &recordingSQLExecutor{result: rowsAffectedResult(1)}
	repo := newAccountRepositoryWithSQL(nil, hit, nil)
	require.NoError(t, repo.SetTempUnschedulable(context.Background(), 5, until, "retry"))
	require.Equal(t, []string{service.EventSeatLimited}, sink.types())
	require.Equal(t, "temp_unschedulable", sink.events[0].Data["kind"])
	require.NotContains(t, sink.events[0].Data, "reason", "the free-text reason stays inside")

	require.NoError(t, repo.ClearTempUnschedulable(context.Background(), 5))
	require.Equal(t, []string{service.EventSeatLimited, service.EventSeatCleared}, sink.types())
}

func TestAccountRepository_AutoPauseEmitsPausedByExpiryPerSeat(t *testing.T) {
	sink := installRepoEventSink(t)
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	now := time.Now()
	mock.ExpectQuery(`(?s)UPDATE accounts.*RETURNING id`).WithArgs(now).
		WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(int64(11)).AddRow(int64(29)))
	mock.ExpectExec(`INSERT INTO scheduler_outbox`).WillReturnResult(sqlmock.NewResult(1, 1))

	n, err := newAccountRepositoryWithSQL(nil, db, nil).AutoPauseExpiredAccounts(context.Background(), now)

	require.NoError(t, err)
	require.EqualValues(t, 2, n)
	require.Equal(t, []string{service.EventSeatPaused, service.EventSeatPaused}, sink.types())
	require.EqualValues(t, 11, sink.events[0].Data["account_id"])
	require.EqualValues(t, 29, sink.events[1].Data["account_id"])
	require.Equal(t, "expiry", sink.events[0].Data["by"])

	sink.events = nil
	mock.ExpectQuery(`(?s)UPDATE accounts.*RETURNING id`).WithArgs(now).WillReturnRows(sqlmock.NewRows([]string{"id"}))
	_, err = newAccountRepositoryWithSQL(nil, db, nil).AutoPauseExpiredAccounts(context.Background(), now)
	require.NoError(t, err)
	require.Empty(t, sink.types())
}

func TestAccountRepository_DefensiveClearsAndRepeatsEmitNothing(t *testing.T) {
	t.Run("set error on an account already in error", func(t *testing.T) {
		sink := installRepoEventSink(t)
		repo, mock := newEntAccountRepoWithMock(t)
		expectStatusRead(mock, service.StatusError)
		expectAccountWrite(mock)
		require.NoError(t, repo.SetError(context.Background(), 5, "401 unauthorized"))
		require.Empty(t, sink.types(), "not a transition")
	})
	t.Run("clear error on a healthy account", func(t *testing.T) {
		sink := installRepoEventSink(t)
		repo, mock := newEntAccountRepoWithMock(t)
		expectStatusRead(mock, service.StatusActive)
		expectAccountWrite(mock)
		require.NoError(t, repo.ClearError(context.Background(), 5))
		require.Empty(t, sink.types())
	})
	t.Run("clear rate limit when nothing was limited", func(t *testing.T) {
		sink := installRepoEventSink(t)
		repo, mock := newEntAccountRepoWithMock(t)
		mock.ExpectExec(`(?s)UPDATE "accounts"`).WillReturnResult(sqlmock.NewResult(0, 0))
		mock.ExpectExec(`(?s)INSERT INTO scheduler_outbox`).WillReturnResult(sqlmock.NewResult(1, 1))
		require.NoError(t, repo.ClearRateLimit(context.Background(), 5))
		require.Empty(t, sink.types())
	})
	t.Run("clear temp unschedulable when nothing was set", func(t *testing.T) {
		sink := installRepoEventSink(t)
		exec := &recordingSQLExecutor{result: rowsAffectedResult(0)}
		require.NoError(t, newAccountRepositoryWithSQL(nil, exec, nil).ClearTempUnschedulable(context.Background(), 5))
		require.Empty(t, sink.types())
	})
}
