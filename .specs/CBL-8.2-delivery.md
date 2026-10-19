# CBL-8.2 (cbl-notification). Signed webhook delivery with retries and the journal

## 1. Context and goal

CBL-8.1 registered merchant endpoints and issued encrypted signing secrets. This
increment delivers: it consumes the platform events (`payment.events`, `payout.events`,
`recon.alerts`), maps them onto the frozen CF-3 merchant types, builds the signed
envelope, delivers it with the contract retry schedule and records the delivery
journal. It unblocks the global scenario GE-6 (unreachable webhook, retries, failed,
manual retry) together with 8.3 and the milestone M2.

## 2. Scope

In scope: the three Kafka consumers through the kit inbox; the event mapping; the
envelope and the signature; the delivery worker with the retry schedule; the
`webhook_deliveries`/`webhook_delivery_attempts` journal; the migrations (outbox,
inbox, deliveries); the service wiring.

Out of scope: manual resend and statistics (8.3); merchant-facing delivery reads (8.3);
the recon alert merchant type (the CF-3 catalog has none; the consumer acks and logs,
recorded in T-NOTIFY-14).

## 3. Architecture of the solution

### 3.1. Consumers and handlers

Three kit-inbox consumers (`payment.events`, `payout.events`, `recon.alerts`; groups
`cbl-notification-payments`/`-payouts`/`-recon`) with the retry topics 5s/1m/10m and
the DLQ. A handler decodes the protobuf event in the inbox transaction, maps it onto
one merchant event (or skips it), finds the merchant endpoints subscribing to its type
and inserts one delivery row per endpoint in the same transaction: the inbox guarantees
the at-least-once consumption, the delivery row guarantees the at-least-once delivery.

### 3.2. Mapping (CF-3)

`payment.events`: DETECTED/CONFIRMING/COMPLETED/EXPIRED/UNDERPAID/OVERPAID/FAILED ->
`invoice.<detected|confirming|completed|expired|underpaid|overpaid|failed>`;
CREATED/UNSPECIFIED -> no webhook. Data: `invoice{id, status, amount, paid_amount, fee,
rate, txid, occurred_at}` (the event amounts as decimal strings of the asset).

`payout.events`: COMPLETED -> `payout.completed` (or `refund.completed` when
`refund_invoice_id` is non-empty); FAILED/COMPENSATED/REJECTED -> `payout.failed` (or
`refund.failed`) with `data.payout.status` = failed/compensated/rejected; REQUESTED/
APPROVED/UNSPECIFIED -> no webhook. Data: `payout{id, status, amount, fee, txid,
failure_reason, occurred_at}` or `refund{id, invoice_id, status, amount, fee, txid,
failure_reason, occurred_at}`.

`recon.alerts`: consumed and acknowledged with a WARN; the CF-3 catalog has no recon
type (T-NOTIFY-14).

### 3.3. Envelope and signature

`{"id":"evt_<uuidv7>","type":"<catalog type>","created_at":"<event time RFC 3339>","data":{...}}`
(one event id per delivery, unchanged across its retries). Headers:
`Content-Type: application/json`,
`X-Billing-Event-Id: <body id>`,
`X-Billing-Signature: t=<unix>,v1=<hex HMAC-SHA256(secretHex, t + "." + body)>` via the
kit `hmac` package; the secret is the delivered hex string as bytes (T-NOTIFY-05).
The body is stored once and re-sent unchanged; only the signature `t` changes per
attempt (T-NOTIFY-11).

### 3.4. Delivery worker and retries

A dispatcher polls due `pending` deliveries every `NOTIFICATION_DISPATCH_INTERVAL`
(default 1 s), claims a batch atomically (an UPDATE with `FOR UPDATE SKIP LOCKED` and a
lease so restarts or parallel instances cannot double-send), sends with the
`NOTIFICATION_ATTEMPT_TIMEOUT` (10 s) and a redirect-free client, and journals every
attempt. A 2xx marks the delivery delivered; otherwise the attempt is scheduled per the
contract schedule 1 s, 5 s, 30 s, 2 min, 15 min, 1 h (7 attempts in total); after the
last failure the delivery is `failed` (T-NOTIFY-10/12/13).

### 3.5. Storage (migration 00004 + the kit tables 00002/00003)

`webhook_deliveries`: `id uuid PK`, `event_id text UNIQUE`, `merchant_id uuid`,
`endpoint_id uuid`, `event_type text`, `body bytea` (the exact signed JSON),
`status text` (pending|delivered|failed), `attempts int`, `next_attempt_at timestamptz`,
`last_error text`, `created_at`, `delivered_at`; the due index on
`(next_attempt_at) WHERE status = 'pending'`.
`webhook_delivery_attempts`: `id bigserial`, `delivery_id uuid`, `attempt int`,
`sent_at`, `http_status int` (0 for a transport error), `error text`, unique
`(delivery_id, attempt)`.
`outbox`/`inbox` from the kit reference DDL.

### 3.6. Decisions

| ID | Decision | Pros | Cons and compensation |
| :-- | :-- | :-- | :-- |
| T-NOTIFY-10 | Deliveries are created in the inbox transaction and sent by a DB-driven worker (no direct send in the consumer) | no lost event, no blocked consumer on slow merchants, retries survive restarts | a small dispatch delay (the interval); the at-least-once semantics need the merchant dedupe by the event id (the contract) |
| T-NOTIFY-11 | The body is built once and stored; the signature timestamp is recomputed per attempt | retries are byte-identical; the merchant dedupe and the signature window work | the stored body duplicates the event payload (bounded by the event size) |
| T-NOTIFY-12 | One delivery (and one `evt_` id) per endpoint per event | independent retry state per merchant URL; the contract id stays stable per delivery | two endpoints of the same merchant receive different ids for the same event (the dedupe is per endpoint, which the contract implies) |
| T-NOTIFY-13 | The delivery client disables redirects, discards the response body (4 KiB cap) and uses the 10 s attempt timeout | no SSRF redirect pivot, no memory blow-up from hostile responses | a merchant relying on redirects fails; the contract says 2xx is success |
| T-NOTIFY-14 | `recon.alerts` is consumed and acked with a WARN (no merchant delivery) | the system contour is wired; no invented CF-3 type | adding a recon type later needs a new consumer group (the inbox dedup); recorded |
| T-NOTIFY-15 | The claim lease covers the whole batch budget (`batch * attemptTimeout + 5 s`) and one dispatcher instance runs on the stand; the status updates are conditional on `status = 'pending'` | no double-send within the lease; a crash interrupts and the lease expires | a second dispatcher instance can only claim rows after the batch budget (the scale-out needs partitioning, 0.7/14.x); a slow merchant delays the batch (the batch is bounded) |
| T-NOTIFY-16 | The consumers start at the topic end (`kgo.NewOffset().AtEnd()`); the delivery row stores the source platform event id and `(source_event_id, endpoint_id)` is unique (the insert skips a replay) | a new group or a group rename cannot deliver old events twice for the same endpoint; the merchant dedupe by the `evt_` id still holds for retries | events produced while the service is down are not delivered (the at-least-once starts at subscribe time; the REST API remains the source of truth); the replay dedupe keys are retained with the journal |
| T-NOTIFY-17 | The delivery-time SSRF guard resolves the merchant host and rejects every non-public address, dialing the validated IP (pinned); the transport errors are classified (timeout/dns/tls/connection/transport) without the delivery address | a merchant URL that resolves to an internal address cannot receive signed bodies (T-NOTIFY-03 is closed); no URL/query token reaches the logs or the journal | the guard adds a DNS resolution per connection; a public IP change between resolution and dial is not re-resolved (pinned, DNS rebinding protection) |
| T-NOTIFY-18 | The untrusted event amounts are validated with the kit `money.New` (decimals range, non-negative minor units) before rendering; only an absent/empty amount is omitted, a present zero renders `0.000000` | a malformed decimals value cannot panic or exhaust the memory (the poison pill becomes an invalid event and a DLQ); the CF-3 Asset regex holds | the amount validation rejects what the producer bug created; the producer errors surface in the inbox retry/DLQ |

## 4. Functional requirements

- FR-1: the payment/payout consumers map the events exactly per 3.2 and create one
  delivery per matching endpoint (merchant + type).
- FR-2: unsupported event types are skipped (acknowledged) with no delivery; a
  non-UUID merchant id or a decode failure is an invalid event (DLQ).
- FR-3: the delivery body/headers follow 3.3 (the envelope id pattern, the signature
  scheme, the event id header).
- FR-4: the worker sends due deliveries, journals every attempt and follows the retry
  schedule; after the last failure the status is failed.
- FR-5: the delivery rows are created in the inbox transaction (the handler receives the
  transaction).
- FR-6: the consumers and the worker are optional at startup (a stand without Kafka
  runs registration only) with a WARN; `KAFKA_BROKERS` enables them.

## 5. Contracts

Frozen: `openapi/webhooks.v1.yaml` (CF-3), `billing/events/v1/{payment,payout,recon}_events.proto`
(CF-2/CF-4). No changes.

## 6. Data model

Section 3.5; migrations 00002 (outbox), 00003 (inbox), 00004 (deliveries).

## 7. State machines and rules

Delivery: pending -> delivered | pending (retry, attempt+1) -> failed after the 7th
failure. The attempt number starts at 1; the next delay is `schedule[attempt-1]`.

## 8. Edge cases

- An event without matching endpoints: no delivery rows (the consumer is ack).
- A merchant with several endpoints subscribing to the same type: one delivery each.
- A zero/absent amount or fee in the event: the asset field is omitted (not zero).
- A payout FAILED/COMPENSATED/REJECTED: the same `payout.failed`/`refund.failed` type,
  the exact state in `data.payout.status`/`data.refund.status`.
- A refund event: the `refund.*` type and `data.refund.invoice_id`.
- A concurrent dispatcher claim: the SKIP LOCKED claim and the lease serialize the work.
- A 3xx/4xx/5xx/timeout: a failure; retries; the `last_error` carries a bounded text.
- An invalid protobuf/DLQ: the inbox retries then the DLQ (the kit).

## 9. Non-functional requirements

Coverage at least 80% (`-coverpkg`); the delivery worker is bounded (batch 20, one
goroutine); the attempt timeout 10 s; the body is the stored bytes (no re-marshaling).

## 10. Test plan

| ID | Case |
| :-- | :-- |
| U-01..U-08 | the payment mapping matrix (each type, skip CREATED/UNSPECIFIED), the payout matrix (complete/failed/compensated/rejected, the refund marker), the envelope and header signature verified with the kit, the asset rendering/omission |
| I-01..I-06 | the storage claim/lease, the attempt journal, the retry schedule and the failed transition, the deliveries migration |
| H-01..H-05 | the handler: one delivery per matching endpoint, the skipped types, the body/type, the invalid event |
| W-01..W-05 | the worker: 2xx delivered, 500 retry/failed after the exhaustion, the signature headers, the redirect rejection |
| E2E | the stand: produce a payment.events event, the endpoint sink verifies the signature and the journal shows delivered; GE-6 (unreachable) is 8.3 |

## 11. Work plan and commit

Red tests -> implementation -> green + coverage + lint -> 4 reviews -> AGENTS/journal ->
commit `CBL-8.2: ...`; the umbrella compose env (KAFKA_BROKERS is already there) and the
GE-6 continuation stay for 8.3.

## 12. Open questions

- The dispatcher scale-out uses the claim lease only (one instance on the stand); a
  partitioned claim is a 0.7/14.x concern.

## 13. Review closure (19.10.2026)

Four review passes on the staged diff; findings closed before the commit:

- the delivery-time SSRF guard (BLOCKING): `GuardedDial` resolves the host, rejects
  every non-public address and dials the pinned validated IP; the redirects are
  disabled and the proxy environment is ignored (T-NOTIFY-17);
- the consumer offset replay (BLOCKING): the groups start at the topic end and the
  delivery row stores the source event id with the `(source_event_id, endpoint_id)`
  unique skip (T-NOTIFY-16);
- the untrusted amount poison pill (BLOCKING): the amounts are validated with the kit
  money (`money.New` plus the non-negative check) before rendering (T-NOTIFY-18);
- the claim lease and the attempt time: the lease covers the batch budget, the
  attempt time is per delivery, the status updates are conditional on `pending`
  (T-NOTIFY-15); the endpoint-not-found is permanent, a transient store error leaves
  the row to the lease, a decrypt fault follows the schedule;
- the URL redaction: the transport errors are classified without the delivery
  address; the journal `last_error` and the logs carry the class (T-NOTIFY-17);
- the event size bound (256 KiB) and the canonical source event id/merchant checks;
- the ownership invariant (the endpoint merchant equals the delivery merchant);
- the tests added: the guard matrix, the classified errors, the retry schedule
  attempts 1..7, the ownership mismatch, the no/multiple endpoints, the handler
  clock, the refund.failed and the untrusted amounts, the storage replay dedupe;
- the spec/AGENTS/env updates and the zero-amount semantics clarified (a present
  zero renders `0.000000`, an absent amount is omitted);
- coverage 83.4% with `-coverpkg` (the 80% target), lint 0, vet and the em dash check
  green; the red run is recorded in the task session and the stand e2e in
  `test/e2e/.runs/20261019t151100z/delivery-summary.md`.
