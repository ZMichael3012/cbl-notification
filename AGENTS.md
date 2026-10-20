# AGENTS.md - cbl-notification

Signed merchant webhooks with retries and a delivery journal.

Mandatory global rules: `../../AGENTS.md`, `../../.specs/CBL-001-development-process.md`,
`../../.specs/CBL-002-repos-and-docker.md`, contracts `../../.specs/CBL-003-contracts.md`.
The em dash (U+2014) is forbidden, only the hyphen is used.

## 1. Boundaries

In scope: webhook endpoint registration, delivery, scheduled retries, manual retry.

Out of scope: other platform services; interaction only through `cbl-protos` contracts
(gRPC), Kafka and the perimeter REST (see `CBL-000`, section 2).

## 2. Contracts

- REST: administrative /healthz, /readyz
- gRPC: NotificationService server (`billing/notification/v1`:
  RegisterWebhookEndpoint - endpoint registration and secret issuance, CBL-8.1;
  ListWebhookDeliveries, GetDeliveryStats, ResendWebhookDelivery - the operator
  journal reads and the manual resend, CBL-8.3, additive CF-3)
- Kafka produces: none
- Kafka consumes: payment.events, payout.events, recon.alerts (CBL-8.2)
- Storage: PostgreSQL notification (`webhook_endpoints` - the merchant endpoint with
  the encrypted signing secret, CBL-8.1; `webhook_deliveries` and
  `webhook_delivery_attempts` - the delivery journal, CBL-8.2; `outbox`/`inbox` - the
  kit reference tables)
- Merchant delivery: contract `openapi/webhooks.v1.yaml` (envelope, type
  catalog, signature, retries; frozen CF-3) - the delivery pipeline is CBL-8.2

Contracts are frozen in `cbl-protos` (CF-1 for gRPC, CF-2/CF-3 for events and
webhooks), see `CBL-004`.

## 3. Architecture

Layers: `internal/transport` -> `internal/usecase` -> `internal/domain` -> `internal/storage`.
Libraries: the standard `CBL-002` set (Gin, GORM + pgx driver, franz-go + pkg/sr, go-redis,
goose, zap). `AutoMigrate` is forbidden, migrations are goose only, forward only.
Delivery pipeline (8.2): `internal/domain/merchant_event.go` maps the payment and
payout events onto the CF-3 types (invoice/payout/refund, the refund marker, the
terminal states, the skipped created/requested/approved, the kit-money amount
validation); `internal/usecase/delivery.go` holds the three kit-inbox handlers (one
delivery row per matching endpoint in the inbox transaction, the source event id
dedupe), the dispatcher (the claim lease, the per-attempt time, the contract retry
schedule 1s/5s/30s/2m/15m/1h, 7 attempts, the conditional status updates) and the
HTTP sender (the kit hmac t/v1 signature over the stored body, the delivery-time
SSRF guard `GuardedDial`, no redirects/proxies, the classified errors without the
delivery address); `internal/storage/deliveries.go` holds the claim
(`FOR UPDATE SKIP LOCKED` + lease), the attempt journal and the delivered/retry/
failed transitions. main wires the consumers (start at the topic end), the
dispatcher and the shutdown order.
Endpoint registration (8.1): `internal/domain/endpoint.go` holds the CF-3 catalog
(`EventCatalog`, 11 types), the event normalization (trim, dedupe, bound 32), the SSRF
URL guards (https only, no credentials, no internal names/IP literals: loopback,
private, link-local, multicast, unspecified, CGNAT, 0.0.0.0/8, IPv6 zones, trailing
dot), the signing secret generation (32 CSPRNG bytes hex) and the AES-256-GCM
encryption (the endpoint id is the AAD, `secret_key_version` 1); `internal/storage`
holds `webhook_endpoints` (unique `(merchant_id, idempotency_key)` and
`(merchant_id, url)`; the events list is JSONB) and the repository; `internal/usecase`
implements the idempotent registration (normalized request hash, the race resolved by
the unique index, the reuse and url conflicts); `internal/transport/grpcserver` maps the
frozen proto, rejects a mismatching `x-merchant-id` metadata element and logs the raw
failure with the request/merchant ids. main wires PG_DSN, migrations, the fail-closed
`NOTIFICATION_SECRET_KEY` and the gRPC server.

## 4. Commands

```powershell
make build        # go build ./...
make test         # go test ./...
make vet          # go vet ./...
make lint         # golangci-lint in Docker
make up           # docker compose -f deploy/docker-compose.yml up -d
make down         # docker compose -f deploy/docker-compose.yml down -v
```

E2E runs follow the `test/e2e/E2E.md` instruction (artifacts in `test/e2e/.runs/<mark>/`); there are no PowerShell run scripts.

## 5. Ports and environment

HTTP `8090`, gRPC `9090`, metrics `9290`.
Variables (applicable to the service): `HTTP_ADDR`, `GRPC_ADDR`, `METRICS_ADDR`,
`PG_DSN` (required), `MIGRATIONS_DIR` (default `./migrations`),
`NOTIFICATION_SECRET_KEY` (required: 64 hex characters = the AES-256 signing secret
encryption key; the service refuses to start without it, T-NOTIFY-01/09),
`KAFKA_BROKERS` (enables the delivery consumers and the dispatcher; empty disables the
pipeline with a WARN, T-NOTIFY-10), `NOTIFICATION_PAYMENTS_GROUP` (default
`cbl-notification-payments`), `NOTIFICATION_PAYOUTS_GROUP` (default
`cbl-notification-payouts`), `NOTIFICATION_RECON_GROUP` (default
`cbl-notification-recon`), `NOTIFICATION_DISPATCH_INTERVAL` (default 1s),
`NOTIFICATION_ATTEMPT_TIMEOUT` (default 10s, it also sizes the claim lease);
`NOTIFICATION_RETRY_SCHEDULE` (comma-separated ascending durations, 1..20 entries;
empty keeps the contract schedule; stands/e2e only, T-NOTIFY-20);
`SCHEMA_REGISTRY_URL`, `REDIS_ADDR` - reserved.

## 6. Development rules

- Pipeline (TDD-first, CBL-001): an SDD spec with the architecture sketch and the full test
  plan (unit and integration matrices, e2e scenario descriptions) -> contracts -> red tests
  with recorded evidence -> implementation -> 4-agent review on the staged diff (no-context,
  architecture, with-context, security - blocking) -> AGENTS.md -> final runs (unit +
  integration + local e2e) -> linters -> commit.
- unit+integration coverage is at least 80%; money scenarios only on real PostgreSQL and
  Kafka (testcontainers); a local e2e run in docker-compose is mandatory.
- Money is int64 in minor units plus decimals only; float is forbidden.
- Godoc docstrings are mandatory in English: the first line starts with the entity name and ends with a period; comments explain why; errors are wrapped with `%w`; panics in business code are forbidden.
- Commit: `CBL-<epic>.<number>: <short message>`, the spec with the same prefix in the same commit.

## 7. SLO and metrics

SLO will be specified in task 14.4. Base metrics: latency p95/p99, error rate, outbox lag,
consumer lag; dashboards after task 0.7.

## 8. Trade-offs

| ID | Decision | Pros | Cons and compensation |
| :-- | :-- | :-- | :-- |
| T-NOTIFY-01 | The signing secret is stored encrypted (AES-256-GCM, the key from `NOTIFICATION_SECRET_KEY`, 32 bytes hex); the delivery signs with it (8.2) and the idempotent replay returns the same response | "not stored in plaintext" holds; the secret stays recoverable for signing; no KMS dependency on the stand | the app key is the single point of compromise (Vault in prod, 0.4/0.6; the stand gitignored `deploy/.env`); a key rotation needs the future update RPC (T-NOTIFY-09) |
| T-NOTIFY-02 | Registration idempotency: unique `(merchant_id, idempotency_key)` plus an order-insensitive hash of the url and the normalized (trimmed, deduplicated, sorted) events; the same identity replays, a changed request answers `idempotency_key_reuse` | a retry of the same logical request replays; a different request cannot reuse the key | the secret is returned again on a replay (the same response by definition); the binding is permanent for the endpoint lifetime (stricter than the REST 24h idempotency window) |
| T-NOTIFY-03 | SSRF guards at registration: https only, no credentials, no internal names (trailing dot normalized), no private/loopback/link-local/multicast/unspecified/CGNAT/0.0.0.0/8 IP literals, no IPv6 zones; the delivery-time resolver guard (DNS, redirects) is 8.2 | the registration rejects the common internal targets without DNS resolution | a hostname resolving to an internal address at delivery time is caught by the 8.2 guard (a release gate there) |
| T-NOTIFY-04 | The event catalog is a domain constant (the 11 CF-3 types); a contract-sync test pins it against the frozen `openapi/webhooks.v1.yaml` enum | the catalog cannot drift silently; unknown types are `InvalidArgument` | a catalog extension needs a code change and the contract revision together |
| T-NOTIFY-05 | The signing key is the hex string as delivered (64 ASCII characters), not the raw bytes | the merchant uses the received value verbatim (the common webhook practice) | a merchant implementing the raw-bytes interpretation fails the HMAC; documented in the spec and the 8.2 verifier |
| T-NOTIFY-06 | The merchant id is a canonical lowercase UUID (the platform identity, T-BA-13) | one identity across services | a non-UUID merchant is `InvalidArgument` |
| T-NOTIFY-07 | The events list is JSONB (GORM `serializer:json`), not `text[]` | the list is read/written as a whole; no extra driver dependency | no SQL array operators; the endpoint sets per merchant are small (the 8.2 lookup filters in process) |
| T-NOTIFY-08 | Unique `(merchant_id, url)` implements the frozen REST 409 `webhook_endpoint_exists`; replacing a destination needs the future update RPC | the documented conflict is expressible; a duplicate destination cannot receive two deliveries | no re-registration to the same URL without the update RPC (recorded) |
| T-NOTIFY-09 | `secret_key_version` is written (always 1) and the keyring stays single-key in 8.1 | the row is ready for a versioned keyring; no invented rotation flow | an ad hoc key change makes existing ciphertexts undecryptable: restore the key, rotate only with the future RPC |
| T-NOTIFY-10 | Deliveries are created in the inbox transaction and sent by a DB-driven worker | no lost event, no blocked consumer, retries survive restarts | a small dispatch delay; the at-least-once needs the merchant dedupe |
| T-NOTIFY-11 | The body is built once and stored; the signature timestamp is recomputed per attempt | retries are byte-identical | the stored body duplicates the payload (bounded) |
| T-NOTIFY-12 | One delivery (and one evt_ id) per endpoint per event | independent retry state; the id stays stable per delivery | two endpoints receive different ids for one event |
| T-NOTIFY-13 | The client disables redirects/proxies, discards the response body and uses the attempt timeout | no redirect pivot, no memory blow-up | a merchant relying on redirects fails (2xx is success) |
| T-NOTIFY-14 | recon.alerts is consumed and acked with a WARN (no merchant type in CF-3) | the contour is wired; no invented type | a future recon type needs a new consumer group (the inbox dedup) |
| T-NOTIFY-15 | The claim lease covers the batch budget (batch x timeout + 5s); one dispatcher on the stand; the status updates are conditional on pending | no double send within the lease | scale-out needs partitioning (0.7/14.x); a slow merchant delays the bounded batch |
| T-NOTIFY-16 | The consumers start at the topic end; the delivery row stores the source event id and (source_event_id, endpoint_id) is unique (a replay is skipped) | a new group or a rename cannot deliver old events twice per endpoint | events produced while the service is down are not delivered (the REST API remains the source of truth) |
| T-NOTIFY-17 | The delivery-time SSRF guard resolves the host, rejects non-public addresses and dials the pinned IP; the transport errors are classified without the URL | T-NOTIFY-03 is closed; no URL/query token in the logs or the journal | a DNS resolution per connection; a public IP change is not re-resolved (anti-rebinding) |
| T-NOTIFY-18 | The event amounts are validated with the kit money (decimals range, non-negative) before rendering | a poison pill becomes an invalid event and a DLQ; the CF-3 Asset regex holds | a producer bug surfaces in the retry/DLQ |
| T-NOTIFY-19 | The resend is structurally idempotent (conditional reset); the idempotency_key is required but not persisted | no extra table; a repeated call cannot duplicate the send | the repeated response reflects later state (still pending) |
| T-NOTIFY-20 | The retry schedule is configurable for stands (NOTIFICATION_RETRY_SCHEDULE) | fast e2e/load exhaustion; the contract default holds in prod | a stand misconfiguration can hide the schedule |
| T-NOTIFY-21 | The listing returns the body SHA-256, not the body | no payloads or secrets in operator UIs | a body inspection needs the DB |
| T-NOTIFY-22 | A resend of a delivered delivery is FAILED_PRECONDITION | no duplicate merchant events with the same evt_ id | a deliberate replay would be a 12.x feature with a new event id |
| T-NOTIFY-23 | attempts (the monotonic journal number) and retry_attempts (the schedule position) are split | the resend restarts the schedule without (delivery_id, attempt) collisions; the journal stays append-only | one more column and the backfill |

## 9. Known issues

| ID | Problem | Impact | Plan | Status |
| :-- | :-- | :-- | :-- | :-- |
| KI-notification-01 | CI and pre-commit are not configured | linters run manually | task 0.2 | open |
| KI-notification-02 | No authN/authZ on the gRPC NotificationService (port 9090) and the port is published by the stand compose: any caller can register an endpoint for any merchant id and receives the signing secret, so 8.2 would deliver that merchant's events to the attacker URL; the transport rejects a mismatching `x-merchant-id` only when the metadata is present | prod deployment is forbidden; do not publish 9090 outside the trusted network | mTLS and authorization 0.5/0.6; the Billing API is the only intended caller; a per-merchant registration cap is a 7.x/15.4 follow-up | open |
| KI-notification-03 | No retention or archive for `webhook_deliveries` (the bodies), `webhook_delivery_attempts` and the kit `inbox` | the journal and the inbox grow with every event and retry | retention/partitioning decision with 14.2; archive the bodies of old delivered/failed rows | open |

## 10. Task journal

| CBL | Date | Status | Spec | Commit |
| :-- | :-- | :-- | :-- | :-- |
| CBL-16.12 | 2026-10-16 | done (English comments and documentation; e2e run instruction `test/e2e/E2E.md` added as a plan, scenarios arrive with CBL-8.x; no code changes) | `.specs/CBL-16.12-conventions.md` | this commit |
| CBL-8.1 | 2026-10-19 | done, review closed (webhook endpoint registration and signing secrets: the frozen NotificationService/RegisterWebhookEndpoint, the CF-3 catalog (11 types) with the contract-sync test, the SSRF URL guards (https, no credentials/internal names/private literals/CGNAT/0.0.0.0/8/IPv6 zones/trailing dot), the 32-byte CSPRNG secret encrypted with AES-256-GCM (the endpoint id is the AAD, `secret_key_version`), the idempotent registration keyed by (merchant, key) with the normalized hash and resolved races, the frozen url conflict (409 `webhook_endpoint_exists`), the `webhook_endpoints` migration, the gRPC server with the mismatching `x-merchant-id` rejection and raw-error logging, the fail-closed startup (non-zero exit); 4 review passes, findings closed (the committed key default removed, JSONB/URL-unique/key-version recorded, the normalized hash, the SSRF gaps, the logging, the tests, the docs); unit+integration+the stand e2e green, coverage 86.0% with -coverpkg, lint 0) | `.specs/CBL-8.1-webhook-endpoints.md` | this commit |
| CBL-8.2 | 2026-10-19 | done, review closed (signed webhook delivery: the payment/payout/recon kit-inbox consumers with the CF-3 mapping (invoice/payout/refund, the refund marker, the terminal states, the skipped types, the kit-money amount validation), the envelope/signature (evt_ ids, the t/v1 hmac over the stored body, the event id header), the DB-driven dispatcher with the claim lease, the per-attempt time and the contract retry schedule (1s/5s/30s/2m/15m/1h, 7 attempts), the delivery journal, the delivery-time SSRF guard (GuardedDial: resolve, reject non-public, pin), the classified transport errors without the URL, the source-event replay dedupe and the AtEnd offset policy, the start-at-end consumers and the ordered shutdown; 4 review passes with 3 blocking findings closed (the SSRF guard, the offset replay, the untrusted decimals poison pill) plus the lease/time-base, the URL redaction and the test matrix; unit+integration+the stand e2e green, coverage 83.4% with -coverpkg, lint 0) | `.specs/CBL-8.2-delivery.md` | this commit |
| CBL-8.3 | 2026-10-19 | done, review closed (manual resend and delivery statistics: ListWebhookDeliveries with the keyset cursor (base64 created_at+uuid), the merchant/endpoint/status/type/window filters, the 90-day window and page bounds; GetDeliveryStats with the 24h default window and the terminal-only success rate; ResendWebhookDelivery with the conditional failed reset, the attempts/retry_attempts split (the journal numbering continues, the schedule round restarts), the pending no-op, the delivered FAILED_PRECONDITION, the concurrent single-winner; the listing index migration 00005; the stand-only NOTIFICATION_RETRY_SCHEDULE with the startup validation; the body digest instead of the body; 4 review passes, the blocking resend-numbering finding closed; unit+integration+the stand e2e green, coverage 82.7% with -coverpkg, lint 0) | `.specs/CBL-8.3-resend-and-stats.md` | this commit |
| CBL-0.9 | 2026-10-20 | done (cbl-kit and cbl-protos are pinned remote modules fetched from the GitHub origin; no local replace directives; Docker builds from the repository root; the lint container fetches the modules via GOPRIVATE and the git insteadOf rewrite) | `../../.specs/CBL-0.9-remote-modules.md` (umbrella) | this commit |

## 11. Runbook

Endpoint registration (8.1):

1. Startup fails with a non-zero exit when `PG_DSN` is empty, `NOTIFICATION_SECRET_KEY`
   is missing or malformed, or migrations fail; the log names the fault (never the key
   material).
2. `RegisterWebhookEndpoint` answers `InvalidArgument` for a bad key/merchant/url/events,
   `AlreadyExists` with the retry semantics for the same key and the same normalized
   request, `AlreadyExists` `idempotency_key_reuse` for the same key with a changed
   request, and `AlreadyExists` `webhook_endpoint_exists` for a URL the merchant
   already registered.
3. The signing secret is returned in the response (and on an idempotent replay); it is
   never logged; the stored ciphertext can only be decrypted with the current
   `NOTIFICATION_SECRET_KEY` (restore the key instead of rotating it ad hoc).
4. The delivery pipeline (8.2) signs with the hex string as delivered (T-NOTIFY-05).
5. `readyz` answers 503 when PostgreSQL is unavailable; the readiness ping is bounded.
6. A decrypt failure on a replay means the key changed: restore the original key (the
   `secret_key_version` column is reserved for the future rotation RPC).

Delivery pipeline (8.2):

1. Disabled pipeline: the startup WARN `kafka is not configured` means `KAFKA_BROKERS`
   is empty (registration only); the consumers log per group.
2. The delivery journal:
   `SELECT id, event_id, event_type, status, attempts, next_attempt_at, last_error
   FROM webhook_deliveries WHERE merchant_id = '...' ORDER BY created_at DESC`; the
   attempts: `SELECT attempt, sent_at, http_status, error FROM
   webhook_delivery_attempts WHERE delivery_id = '...' ORDER BY attempt`.
3. The retry schedule is the contract (1s, 5s, 30s, 2m, 15m, 1h; 7 attempts); the
   `last_error` carries a bounded class (timeout/dns/tls/connection/transport/status),
   never the delivery URL.
4. A delivery stuck in `pending` with a future `next_attempt_at` is waiting for the
   schedule (or the claim lease after a crash); the dispatcher reclaims it after the
   lease.
5. `ErrBlockedAddress` in the log means the merchant host now resolves to an internal
   address (the delivery-time SSRF guard); the delivery follows the schedule and
   fails; investigate the endpoint URL.
6. A failed delivery (8.3) is reset for a manual resend; the journal stays append-only.
7. The consumer offset policy: the groups start at the topic end (T-NOTIFY-16); a
   renamed group replays from the end, and the `(source_event_id, endpoint_id)` unique
   skips the deliveries already created for an endpoint.

Operator delivery reads and resend (8.3):

1. The listing: `ListWebhookDeliveries` scoped by `merchant_id`, ordered `created_at
   DESC, id DESC`, keyset cursor in `page_token`; the response carries the body
   SHA-256, not the body (T-NOTIFY-21). The RPCs inherit KI-notification-02 (no
   authN): do not expose 9090 outside the trusted network.
2. The statistics: `GetDeliveryStats` defaults to `now-24h..now`; the success rate
   counts `delivered / (delivered + failed)` only.
3. The resend: `ResendWebhookDelivery` only resets a `failed` row (the conditional
   update, single-winner); a pending row is a no-op; a delivered row is
   `FAILED_PRECONDITION` (T-NOTIFY-22); the attempt journal is append-only and the
   number continues across rounds (T-NOTIFY-23).
4. The schedule override: `NOTIFICATION_RETRY_SCHEDULE=1s` on the stand makes the
   exhaustion fast for GE-6; an invalid value fails the startup (fail-closed).

Typical incidents: dependency unavailability, error growth, processing lag (8.2);
actions: check health/metrics, logs with request_id, the Kafka consumer group and outbox
lag, restart the service if needed.
