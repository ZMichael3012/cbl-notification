# CBL-8.3 (cbl-notification). Manual webhook resend and delivery statistics

## 1. Context and goal

CBL-8.1 registered merchant endpoints; CBL-8.2 delivered signed webhooks with the
contract retry schedule and the journal (`webhook_deliveries`,
`webhook_delivery_attempts`). This increment exposes the journal to the operator
surface: a manual resend of a failed delivery and delivery statistics. It closes the
last E8 task and the notification half of GE-6 (unreachable webhook, retries, failed,
manual retry).

## 2. Scope

In scope: three additive gRPC methods of `NotificationService` (CF-3.2):
`ListWebhookDeliveries`, `GetDeliveryStats`, `ResendWebhookDelivery`; the storage
queries (keyset listing, status aggregates, the conditional resend reset); the
listing index migration; the resend semantics; the configurable retry schedule for
stands and the e2e.

Out of scope: the merchant/operator REST (Billing API 7.x, Back-office 12.x consume
the RPCs); authentication of the RPCs (KI-notification-02); retention (KI-notification-03);
a resend of a delivered event (deliberately rejected, see 3.3).

## 3. Architecture of the solution

### 3.1. Contract (additive, backward compatible, CF-3.2)

```proto
service NotificationService {
  rpc RegisterWebhookEndpoint(RegisterWebhookEndpointRequest) returns (RegisterWebhookEndpointResponse);
  rpc ListWebhookDeliveries(ListWebhookDeliveriesRequest) returns (ListWebhookDeliveriesResponse);
  rpc GetDeliveryStats(GetDeliveryStatsRequest) returns (GetDeliveryStatsResponse);
  rpc ResendWebhookDelivery(ResendWebhookDeliveryRequest) returns (ResendWebhookDeliveryResponse);
}
```

`ListWebhookDeliveriesRequest`: `merchant_id` (required), `endpoint_id`, `status`,
`event_type`, `from`, `to` (optional filters), `page_size` (1..100, default 20),
`page_token` (opaque keyset cursor). Response: `deliveries[]` (`delivery_id`,
`event_id`, `endpoint_id`, `event_type`, `status`, `attempts`, `next_attempt_at`,
`last_error`, `created_at`, `delivered_at`, `body_sha256`), `next_page_token`.

`GetDeliveryStatsRequest`: `merchant_id` (required), `from`, `to`. Response: `total`,
`pending`, `delivered`, `failed`, `success_rate` (delivered / (delivered + failed),
0 when no terminal delivery).

`ResendWebhookDeliveryRequest`: `delivery_id` (required), `idempotency_key`
(required by the global rule; the reset is structurally idempotent, T-NOTIFY-19).
Response: `delivery_id`, `status`, `attempts`, `next_attempt_at`.

The listing response carries the signed body's SHA-256, not the body: the back-office
needs an identity of the encrypted-at-rest payload without re-emitting signing
secrets or merchant payloads into operator UIs.

### 3.2. Listing and statistics

The listing is merchant-scoped and ordered `created_at DESC, id DESC`; the cursor is
`base64url(created_at RFC3339Nano + "|" + id)` of the last row, and the query uses the
tuple comparison `(created_at, id) < (?, ?)`. Invalid cursors and filters are
`INVALID_ARGUMENT`. `from` is inclusive, `to` is exclusive; a list/stats window is
bounded to 90 days, `from <= to`; missing `from`/`to` mean no bound for the list and
`now-24h`/`now` for the statistics.

The statistics aggregate groups the rows by status in the window with a single query;
`success_rate` is a ratio (not money; float is allowed for rates).

Storage: migration 00005 adds `webhook_deliveries_listing_idx ON (merchant_id,
created_at DESC, id DESC)`; the same index serves the listing and the statistics
range scan. The due index 00004 remains for the dispatcher.

### 3.3. Resend semantics

A resend is a conditional state reset of a `failed` delivery:
`UPDATE ... SET status='pending', retry_attempts=0, next_attempt_at=now, last_error=''
WHERE id=? AND status='failed' RETURNING ...`.

- `failed` -> reset to `pending`, due immediately; the dispatcher sends it per the
  schedule from scratch. Two counters split the concerns: `attempts` is the total
  performed attempts (the journal number, monotonic across resends, the journal stays
  append-only and conflict-free) and `retry_attempts` is the position in the retry
  schedule (reset by a resend; migration 00005 adds it, existing rows are backfilled
  from `attempts`).
- `pending` -> no-op, returns the current row (a repeated resend cannot duplicate the
  send).
- `delivered` -> `FAILED_PRECONDITION`: an automatic resend of a successful delivery
  would duplicate a merchant-visible event with the same `evt_` id; a deliberate
  operator replay is a separate 12.x feature.
- unknown id -> `NOT_FOUND`; empty id/key -> `INVALID_ARGUMENT`.
- The conditional update makes concurrent resends single-winner; the loser re-reads
  and returns `pending`.

### 3.4. Configurable retry schedule (stands, e2e, load)

`NOTIFICATION_RETRY_SCHEDULE` (comma-separated Go durations, ascending, 1..20 entries)
overrides the contract schedule for stands and the e2e; the default stays the
contract `1s,5s,30s,2m,15m,1h`. Production runs without the override (T-NOTIFY-20).

## 4. Error taxonomy

| Condition | gRPC code |
| :-- | :-- |
| missing/empty `merchant_id`, `delivery_id`, `idempotency_key`; bad status/event type; bad cursor; bad window | INVALID_ARGUMENT |
| unknown delivery id | NOT_FOUND |
| resend of a delivered delivery | FAILED_PRECONDITION |
| storage failure | INTERNAL |

## 5. Storage changes

Migration 00005: `ALTER TABLE webhook_deliveries ADD COLUMN retry_attempts INT NOT
NULL DEFAULT 0` with the backfill `UPDATE ... SET retry_attempts = attempts`, and
`CREATE INDEX webhook_deliveries_listing_idx ON (merchant_id, created_at DESC, id
DESC)`. The resend uses the existing columns; `attempts` stays the monotonic total of
performed attempts, `retry_attempts` is the schedule position (reset by a resend).

## 6. Test design

Red run (TDD, step 5): `go test ./internal/...` failed to build (`undefined:
storage.ListQuery`, `undefined: usecase.NewAdminService`, `RetryAttempts` missing),
recorded before the implementation.

### 6.1. Unit (no Docker) - `internal/usecase/admin_test.go`, `admin_test.go` schedule cases

| ID | Case | Expected |
| :-- | :-- | :-- |
| T-NOTIFY-19-01 | resend a failed delivery | pending, retry position reset, attempts unchanged, next_attempt_at=now, last_error empty |
| T-NOTIFY-19-02 | resend a pending delivery | no-op, the current row returned, no reset |
| T-NOTIFY-19-03 | resend a delivered delivery | FAILED_PRECONDITION |
| T-NOTIFY-19-04 | resend an unknown delivery | NOT_FOUND |
| T-NOTIFY-19-05 | empty delivery id / idempotency key | INVALID_ARGUMENT |
| T-NOTIFY-19-06 | list: page size default 20, clamp 1..100, over-range rejected | INVALID_ARGUMENT |
| T-NOTIFY-19-07 | list: bad status / bad event type / bad cursor | INVALID_ARGUMENT |
| T-NOTIFY-19-08 | list: cursor roundtrip (encode/decode the last row) | the same tuple |
| T-NOTIFY-19-09 | list: window bounds (from > to, > 90 days) | INVALID_ARGUMENT |
| T-NOTIFY-19-10 | stats: defaults (now-24h..now), rate 0 without terminal, rate computed | the expected counts |
| T-NOTIFY-19-11 | retry schedule parsing: default, override, descending/empty/zero/bad | default or INVALID_ARGUMENT at startup |

### 6.2. Integration (PostgreSQL) - `internal/storage/deliveries_integration_test.go`

| ID | Case | Expected |
| :-- | :-- | :-- |
| T-NOTIFY-19-20 | list: order and keyset pagination over 5 rows (page size 2) | 2+2+1, no duplicates, DESC |
| T-NOTIFY-19-21 | list: filters (endpoint, status, type, window) | only matching rows |
| T-NOTIFY-19-22 | stats: counts by status in the window; out-of-window excluded | the expected counts and rate |
| T-NOTIFY-19-23 | resend: the failed row resets; a concurrent second resend changes nothing | one reset, both calls return pending |
| T-NOTIFY-19-24 | resend: the attempt journal is preserved; the next attempt number continues | attempts monotonic |
| T-NOTIFY-19-25 | the dispatcher claims the resent row immediately | a send happens |

### 6.3. Transport - `internal/transport/grpcserver/server_test.go`

| ID | Case | Expected |
| :-- | :-- | :-- |
| T-NOTIFY-19-30 | the three methods map the request/response | the proto fields |
| T-NOTIFY-19-31 | the error mapping (NOT_FOUND, FAILED_PRECONDITION, INVALID_ARGUMENT) | the codes |

### 6.4. E2E (global stand, GE-6 continuation)

1. Register an endpoint with an unreachable https URL (the existing stand endpoint).
2. Produce a `payment.completed` event; with `NOTIFICATION_RETRY_SCHEDULE=1s,1s,1s,1s,1s,1s`
   the dispatcher exhausts 7 attempts quickly; the delivery becomes `failed`.
3. `GetDeliveryStats` shows total 1, failed 1.
4. `ResendWebhookDelivery` -> pending, attempts reset; the dispatcher retries and the
   delivery fails again after the schedule.
5. `ListWebhookDeliveries` shows the row and its attempts; the journal keeps both
   rounds.
6. The artifacts go to `test/e2e/.runs/<mark>/ge-6*.txt`.

## 7. Decisions

| ID | Decision | Pros | Cons |
| :-- | :-- | :-- | :-- |
| T-NOTIFY-19 | The resend is structurally idempotent (conditional reset); the `idempotency_key` is accepted but not persisted | no extra table; a repeated call cannot duplicate the send | the response of a repeated call reflects later state (still pending) |
| T-NOTIFY-20 | The retry schedule is configurable for stands | fast e2e/load exhaustion; the contract default holds in prod | a stand misconfiguration can hide the schedule; the production env does not set it |
| T-NOTIFY-21 | The listing returns the body hash, not the body | no payloads/secrets in operator UIs; the journal identity is verifiable | a body inspection needs the DB (operator-only) |
| T-NOTIFY-22 | A resend of a delivered delivery is rejected | no duplicate merchant events with the same evt_ id | a deliberate replay (if ever needed) is a new 12.x feature with a new event id |
| T-NOTIFY-23 | `attempts` (monotonic journal number) and `retry_attempts` (schedule position) are split | the resend restarts the schedule without `(delivery_id, attempt)` collisions; the journal stays append-only | one more column and one backfill |

## 8. Results (19.10.2026)

- Unit: the admin semantics (resend failed/pending/delivered/unknown/validation, page
  bounds and filters, cursor roundtrip, window rules, stats defaults and rate, schedule
  parsing) green.
- Integration (PostgreSQL, migrations 00001..00005): keyset pages 2+2+1 DESC without
  duplicates, filters and stats window, the resend reset with the journal preserved
  (attempt 8 continues), a concurrent double resend resets once, the dispatcher claims
  the resent row.
- Transport (bufconn): the three methods map the proto fields, the body digest, the
  optional timestamps; NOT_FOUND/FAILED_PRECONDITION/INVALID_ARGUMENT map to the codes.
- Coverage 82.7% with `-coverpkg`, golangci-lint 0, go vet clean, em dash check green.
- Stand e2e (GE-6 continuation, `NOTIFICATION_RETRY_SCHEDULE=1s`): the unreachable
  endpoint exhausts the retries, the stats show failed, the manual resend resets the
  round and the delivery fails again; the artifacts are in
  `test/e2e/.runs/20261019t130356z/ge-6*.txt` (the umbrella).

## 9. Review closure (19.10.2026)

Four review passes on the staged diff; the findings closed:

- the resend attempt numbering (BLOCKING): the naive `attempts=0` reset would collide
  with the append-only journal; the split of `attempts`/`retry_attempts` (T-NOTIFY-23)
  fixes the round restart while the journal numbering continues;
- the concurrent resend: the conditional update plus the re-read makes it
  single-winner; the loser returns the pending state;
- the delivered resend is rejected (T-NOTIFY-22), the unknown id is NOT_FOUND, the
  missing identities are INVALID_ARGUMENT;
- the listing cursor is validated (base64, timestamp, UUID), the filters are checked
  against the catalog, the window is bounded (90 days), the statistics defaults are
  fixed (24h) and the rate is computed only over terminal rows;
- the transport validates the untrusted timestamps (`CheckValid`) and maps the errors;
- the `idempotency_key` is required by the contract rule but not persisted, documented
  (T-NOTIFY-19);
- the retry schedule override is a stand-only knob (`NOTIFICATION_RETRY_SCHEDULE`),
  strictly ascending, bounded 20 entries, fail-closed at startup (T-NOTIFY-20);
- the body is never returned by the listing (the digest only, T-NOTIFY-21); the
  security reviewer notes the RPCs inherit KI-notification-02 (no authN): the operator
  reads expose merchant journal metadata, so the stand-only exposure is confirmed in
  the AGENTS runbook.
