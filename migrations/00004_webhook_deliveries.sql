-- 00004: merchant webhook delivery journal (CBL-8.2).
-- A delivery row is created in the consumer inbox transaction; the dispatcher
-- claims due rows with a lease (FOR UPDATE SKIP LOCKED) and journals every
-- attempt. The body is the exact signed JSON and never changes across retries.

-- +goose Up
CREATE TABLE webhook_deliveries (
    id UUID PRIMARY KEY,
    event_id TEXT NOT NULL UNIQUE,
    source_event_id UUID NOT NULL,
    merchant_id UUID NOT NULL,
    endpoint_id UUID NOT NULL REFERENCES webhook_endpoints(id),
    event_type TEXT NOT NULL,
    body BYTEA NOT NULL,
    status TEXT NOT NULL,
    attempts INT NOT NULL DEFAULT 0,
    next_attempt_at TIMESTAMPTZ,
    last_error TEXT NOT NULL DEFAULT '',
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    delivered_at TIMESTAMPTZ,
    CONSTRAINT webhook_deliveries_source_endpoint_uniq UNIQUE (source_event_id, endpoint_id)
);

CREATE INDEX webhook_deliveries_due_idx ON webhook_deliveries (next_attempt_at)
    WHERE status = 'pending';

CREATE TABLE webhook_delivery_attempts (
    id BIGSERIAL PRIMARY KEY,
    delivery_id UUID NOT NULL REFERENCES webhook_deliveries(id),
    attempt INT NOT NULL,
    sent_at TIMESTAMPTZ NOT NULL,
    http_status INT NOT NULL,
    error TEXT NOT NULL DEFAULT '',
    CONSTRAINT webhook_delivery_attempts_uniq UNIQUE (delivery_id, attempt)
);

-- +goose Down
DROP TABLE webhook_delivery_attempts;
DROP TABLE webhook_deliveries;
