-- 00005: delivery listing and manual resend (CBL-8.3).
-- attempts stays the monotonic total of performed attempts (the journal
-- numbering); retry_attempts is the position in the retry schedule and is
-- reset by a manual resend. Existing rows are backfilled to attempts.

-- +goose Up
ALTER TABLE webhook_deliveries ADD COLUMN retry_attempts INT NOT NULL DEFAULT 0;
UPDATE webhook_deliveries SET retry_attempts = attempts;
CREATE INDEX webhook_deliveries_listing_idx ON webhook_deliveries (merchant_id, created_at DESC, id DESC);

-- +goose Down
DROP INDEX webhook_deliveries_listing_idx;
ALTER TABLE webhook_deliveries DROP COLUMN retry_attempts;
