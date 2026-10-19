-- 00001: webhook endpoint registrations of the notification service (CBL-8.1).
-- The signing secret is stored encrypted (AES-256-GCM, the endpoint id is the
-- AAD, the key version reserves the rotation path); the (merchant_id,
-- idempotency_key) pair is the registration identity and (merchant_id, url)
-- enforces the frozen REST rule "one active endpoint per url" (409
-- webhook_endpoint_exists). The events list is JSONB (T-NOTIFY-07).

-- +goose Up
CREATE TABLE webhook_endpoints (
    id UUID PRIMARY KEY,
    merchant_id UUID NOT NULL,
    url TEXT NOT NULL,
    events JSONB NOT NULL,
    secret_ciphertext BYTEA NOT NULL,
    secret_nonce BYTEA NOT NULL,
    secret_key_version INT NOT NULL DEFAULT 1,
    idempotency_key TEXT NOT NULL,
    request_hash TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE UNIQUE INDEX webhook_endpoints_merchant_idempotency_idx
    ON webhook_endpoints (merchant_id, idempotency_key);

CREATE UNIQUE INDEX webhook_endpoints_merchant_url_idx
    ON webhook_endpoints (merchant_id, url);

-- +goose Down
DROP TABLE webhook_endpoints;
