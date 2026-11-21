-- Outgoing events webhook (SC-28): events wait here until the leader-locked
-- worker has delivered them (or 24 hours of retries have run out).
CREATE TABLE IF NOT EXISTS event_outbox (
    id              VARCHAR(64)  PRIMARY KEY,
    event_type      VARCHAR(64)  NOT NULL,
    body            JSONB        NOT NULL,
    status          VARCHAR(16)  NOT NULL DEFAULT 'pending',
    attempts        INT          NOT NULL DEFAULT 0,
    last_error      VARCHAR(64),
    created_at      TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    next_attempt_at TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    finished_at     TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_event_outbox_due ON event_outbox (next_attempt_at) WHERE status = 'pending';
CREATE INDEX IF NOT EXISTS idx_event_outbox_finished ON event_outbox (finished_at) WHERE status <> 'pending';

COMMENT ON TABLE event_outbox IS 'Outgoing events webhook outbox; body is the signed JSON envelope, last_error is a class (http_500, timeout), never vendor text';
