# CBL-8.1 (cbl-notification). Webhook endpoint registration and signing secrets

## 1. Context and goal

Epic E8 (W3-NOTIFY) was never implemented: the repository is the CBL-0.1 scaffold, the
global E2E scenario GE-6 and the milestone M2 (payouts, reconciliation, notifications
and reporting on the stand) are blocked. This task is the first increment: merchant
webhook endpoint registration and signing secret storage via the frozen gRPC contract
`billing.notification.v1.NotificationService/RegisterWebhookEndpoint` (CF-3), so the
delivery pipeline (8.2) and the manual resend (8.3) have endpoints and secrets to work
with.

## 2. Scope

In scope:

- the gRPC server implementation of `RegisterWebhookEndpoint`: request validation
  (merchant UUID, https URL with SSRF guards, the CF-3 event catalog), idempotent
  registration, signing secret issuance and storage;
- the `webhook_endpoints` storage (migration 00001) with the encrypted signing secret;
- the secret encryption at rest (AES-256-GCM, `NOTIFICATION_SECRET_KEY`);
- main wiring: PostgreSQL, migrations, the gRPC server registration, the fail-closed
  configuration.

Out of scope:

- delivery, retries, the delivery journal (8.2); the event consumers (8.2);
- manual resend and statistics (8.3);
- the merchant-facing REST `POST /v1/webhook-endpoints` (the Billing API proxy of
  `billing.v1.yaml`, a 7.x/15.4 concern; the stand and GE-6 register through gRPC);
- endpoint update/delete/list (not in the frozen proto; a future additive contract).

## 3. Architecture of the solution

### 3.1. Packages and layers

`internal/transport` (gRPC server) -> `internal/usecase` (registration, idempotency)
-> `internal/domain` (endpoint, catalog, URL rules, secret) -> `internal/storage`
(GORM repository, migration 00001). `cmd/notification` wires PG_DSN, migrations, the
secret key and the gRPC server.

### 3.2. Contract (frozen)

`RegisterWebhookEndpointRequest{idempotency_key, merchant_id, url, events[]}` ->
`RegisterWebhookEndpointResponse{endpoint_id, url, events[], signing_secret}`. The
signing secret is shown once and is not stored in plaintext.

### 3.3. Data flows

The Billing API (later) or the stand registers an endpoint with an idempotency key; the
service validates the request, generates a 32-byte CSPRNG secret (hex), encrypts it with
AES-256-GCM and stores the endpoint; the response carries the secret. A retry with the
same key and the same request hash returns the stored endpoint and the decrypted secret
(the same response, idempotent); a different hash under the same key answers
`AlreadyExists`.

### 3.4. Storage

`webhook_endpoints` (migration 00001): `id uuid PK` (UUIDv7), `merchant_id uuid NOT
NULL`, `url text NOT NULL`, `events jsonb NOT NULL`, `secret_ciphertext bytea NOT
NULL`, `secret_nonce bytea NOT NULL`, `secret_key_version int NOT NULL DEFAULT 1`,
`idempotency_key text NOT NULL`, `request_hash text NOT NULL`, `created_at timestamptz
NOT NULL DEFAULT now()`; unique indexes `webhook_endpoints_merchant_idempotency_idx
(merchant_id, idempotency_key)` (the registration identity) and
`webhook_endpoints_merchant_url_idx (merchant_id, url)` (the frozen REST rule "one
active endpoint per url", 409 `webhook_endpoint_exists`). Forward-only goose migration.
The events list is JSONB (T-NOTIFY-07): the list is always read and written as a whole,
the pgx driver does not map `[]string` to `text[]` without an extra dependency, and the
delivery lookup filters in process (endpoint sets per merchant are small).

### 3.5. Error taxonomy

| Case | gRPC code |
| :-- | :-- |
| Missing/duplicate/invalid idempotency key, invalid merchant UUID, bad URL, unknown/empty event types, overlong fields | `InvalidArgument` |
| The idempotency key reused with a different request | `AlreadyExists` (`idempotency_key_reuse`) |
| A fresh key for a URL the merchant already registered | `AlreadyExists` (`webhook_endpoint_exists`, the frozen REST rule) |
| A mismatching `x-merchant-id` metadata element | `Unauthenticated` (`merchant_mismatch`) |
| A storage failure, a decrypt failure | `Internal` (sanitized; the raw error is logged with the request id and the merchant id) |
| A missing `NOTIFICATION_SECRET_KEY` at startup | startup failure with a non-zero exit (fail closed) |

### 3.6. Decisions and rejected alternatives

| ID | Decision | Pros | Cons and compensation |
| :-- | :-- | :-- | :-- |
| T-NOTIFY-01 | The signing secret is stored encrypted (AES-256-GCM, the key from `NOTIFICATION_SECRET_KEY`, 32 bytes hex): the delivery signs with it (8.2) and an idempotent replay can return the same response | "not stored in plaintext" holds; the secret stays recoverable for signing; no KMS dependency on the stand | the app key is the single point of compromise (Vault in prod, 0.4/0.6; the stand `.env`); a key rotation needs a re-registration (recorded in the runbook) |
| T-NOTIFY-02 | Registration idempotency: unique `(merchant_id, idempotency_key)` plus a request hash of (url, sorted events); the same key+hash returns the original endpoint with the decrypted secret; a different hash answers `AlreadyExists` | the platform-wide idempotency semantics (CBL-003 section 7) hold; a retry cannot create a second endpoint | the secret is returned again on a replay (the same response by definition); the response carries it, so the client must treat responses as secret |
| T-NOTIFY-03 | SSRF guards at registration: https only, host bounded, no IP literals in loopback/private/link-local/unspecified ranges, no `localhost`/`.local`/`.internal` names; the delivery-time re-validation (DNS resolution, redirects disabled) lands in 8.2 | the registration rejects the obvious internal targets without DNS resolution (no TOCTOU at registration) | a hostname resolving to a private IP at delivery time is caught by the 8.2 guard; the record is documented here |
| T-NOTIFY-04 | The event catalog is a domain constant (the CF-3 list of 11 types): unknown types are `InvalidArgument`, duplicates are deduplicated, an empty list is rejected | the catalog is one source of truth; the delivery (8.2) maps platform events onto it | a catalog extension is a code change (an additive contract revision); the mock/stand stays in sync by tests |
| T-NOTIFY-05 | The signing key is the hex string as delivered (64 ASCII characters), not the raw bytes | the merchant uses the received value verbatim (the common webhook practice); no encoding ambiguity on the wire | a merchant implementing the raw-bytes interpretation fails the HMAC; documented explicitly in the spec and the 8.2/e2e verifier |
| T-NOTIFY-06 | The merchant id is a canonical lowercase UUID (the platform identity, T-BA-13) | one identity across services; no second mapping space | a non-UUID merchant is `InvalidArgument` (the Billing API issues UUID keys) |
| T-NOTIFY-07 | The events list is stored as JSONB (GORM `serializer:json`), not `text[]` | the list is read/written as a whole; no extra driver dependency; the delivery lookup filters in process | no SQL array operators or GIN index; the endpoint sets per merchant are small and the lookup is by merchant, so the trade-off holds until a per-type SQL filter is required (8.2 measures) |
| T-NOTIFY-08 | Unique `(merchant_id, url)` enforces the frozen REST rule "one active endpoint per url" (409 `webhook_endpoint_exists`); an update/rotation RPC will need the additive contract | the Billing API proxy can answer the documented 409; a duplicate destination cannot silently receive two deliveries | a merchant cannot register the same URL twice (by design); replacing the destination needs the future update RPC (recorded) |
| T-NOTIFY-09 | `secret_key_version` is written now (always 1) and the keyring stays single-key in 8.1; rotation = a future update/re-registration RPC that re-encrypts | the row is ready for a versioned keyring without another migration; no invented rotation flow before its contract exists | a key change before the rotation RPC makes existing ciphertexts undecryptable (the replay answers Internal; the operational runbook: restore the key, do not rotate ad hoc) |

## 4. Functional requirements

- FR-1: `RegisterWebhookEndpoint` validates: idempotency key 8-128 chars
  `[A-Za-z0-9._~-]` (no trimming: the raw key is validated); merchant id a canonical
  UUID; url https, <= 2048 chars, with the SSRF guards; events non-empty, each in the
  CF-3 catalog, deduplicated, <= 32 (the frozen REST bound).
- FR-2: a successful registration generates a 32-byte CSPRNG secret (lowercase hex,
  64 chars), encrypts it (AES-256-GCM with a random nonce, `secret_key_version` 1),
  stores the endpoint and returns the secret.
- FR-3: an idempotent replay (same merchant + key + normalized request hash) returns
  the stored endpoint and the decrypted secret; a hash mismatch is `AlreadyExists`
  (`idempotency_key_reuse`); a fresh key for an already registered URL is
  `AlreadyExists` (`webhook_endpoint_exists`, T-NOTIFY-08).
- FR-4: the secret never appears in logs, errors or the storage plaintext; the
  ciphertext is bound to the endpoint row (the AAD carries the endpoint id).
- FR-5: the service fails closed at startup with a non-zero exit without
  `NOTIFICATION_SECRET_KEY` or with a malformed one; migrations are applied at startup
  (`MIGRATIONS_DIR`).

## 5. Contracts

`cbl-protos` (frozen): `billing/notification/v1/notification.proto` (CF-3),
`openapi/webhooks.v1.yaml` (the catalog and signature scheme, consumed by 8.2). No
contract changes.

## 6. Data model and migrations

Section 3.4; migration `00001_webhook_endpoints.sql` (goose, forward only).

## 7. State machines and rules

No state machine; the endpoint row is immutable in this increment. The secret lifecycle:
generated once per endpoint; a rotation is a re-registration (a new endpoint or a future
update RPC).

## 8. Edge cases

- A duplicate event type (or whitespace-padded types) in the request: deduplicated and
  trimmed; a canonical retry of the same logical request replays (the hash uses the
  normalized set).
- A URL with userinfo (`https://user:pass@host/`): rejected (no credentials in the URL).
- A URL with a non-default port: allowed (the port is part of the target).
- An IP literal in a private range (`https://10.0.0.1/`): rejected; the explicit ranges
  include 0.0.0.0/8 and CGNAT 100.64.0.0/10 (not covered by `IsPrivate`).
- An IPv6 literal: loopback, link-local and zoned forms rejected; a public literal is
  allowed.
- Trailing-dot and loopback names (`localhost.`, `foo.local.`, `foo.localhost`):
  rejected (the host is normalized by trimming one trailing dot).
- A 2049-char URL / 33 event types / a 129-char key: `InvalidArgument`; the exact
  boundary values pass.
- The idempotency key reused with a different URL or a different event set:
  `AlreadyExists` (`idempotency_key_reuse`); with the same URL but a different event
  ORDER: the same request (the hash sorts).
- A fresh idempotency key for a URL the merchant already registered:
  `AlreadyExists` (`webhook_endpoint_exists`, T-NOTIFY-08).
- A concurrent registration of the same identity: the unique index decides; the loser
  re-reads and replays (or answers the URL conflict).
- A merchant id in uppercase: rejected (`InvalidArgument`): the platform identity is
  canonical lowercase (T-NOTIFY-06).
- A mismatching `x-merchant-id` metadata element: `Unauthenticated` (defense in
  depth); absent metadata is tolerated on the stand.

## 9. Non-functional requirements

- Unit+integration coverage at least 80%; the storage integration on real PostgreSQL
  (testcontainers).
- No secrets in logs: the secret is only in the response; the log carries the endpoint
  id and the merchant id.
- The gRPC handler respects the 2 s contract deadline (the validation is in-process).

## 10. Test plan

### 10.1. Unit matrix

| ID | Case | Expected |
| :-- | :-- | :-- |
| U-01 | Event catalog: every CF-3 type is known; an unknown type is not | the exact 11-type list |
| U-02 | URL validation: https ok; http/ftp/empty/userinfo/private IP/loopback/IPv6 loopback/localhost/.local rejected; public IPv6 ok | the guard matrix |
| U-03 | Secret generation: 64 lowercase hex chars, two calls differ | CSPRNG, format |
| U-04 | Secret encryption round trip; a wrong key / tampered ciphertext fails | AES-GCM, AAD = endpoint id |
| U-05 | Request validation: key shape, merchant UUID, events bounds/duplicates | the domain errors |

### 10.2. Integration matrix (PostgreSQL, testcontainers)

| ID | Case | Expected |
| :-- | :-- | :-- |
| I-01 | Register -> the row exists with the encrypted secret (no plaintext match) | the storage round trip |
| I-02 | Idempotent replay: the same key+hash returns the same endpoint id and secret | idempotency |
| I-03 | Key reuse with a different hash: `AlreadyExists` | the conflict |
| I-04 | Two merchants may use the same key | the unique scope |
| I-05 | Migrations apply cleanly (goose version 1) | the schema |

### 10.3. Smoke set

The service starts with the scaffold env (plus PG_DSN + the secret key), `/healthz` and
`/readyz` answer, the gRPC health check is SERVING.

### 10.4. E2E scenario descriptions

- Local (compose stand): register via grpcurl against `:9090`, assert the response
  fields, the replay, the row state (masked secret) - artifacts in
  `test/e2e/.runs/<mark>/`.
- Global (GE-6, after 8.2): the merchant sink registration is the first step of the
  notification scenario; the run is recorded in the umbrella run directory.

### 10.5. Load plan

Not applicable in 8.1 (the registration is an operator/merchant rare call); the delivery
load belongs to 8.2.

### 10.6. Test-first evidence

Red run recorded before the implementation: `go test ./internal/...` fails to compile
on the scaffold (`domain.EventCatalog`, `domain.RegisterRequest`,
`storage.EndpointRecord`, `storage.ErrConflict`/`ErrNotFound`,
`usecase.RegisterService` undefined; the transport package setup fails); the transcript
is kept in the task session. 10.7 records the review closure.

## 11. Work plan and commit

1. This spec.
2. Dependencies: gorm+pgx, goose, uuid, testify, testcontainers, cbl-kit/cbl-protos
   replaces (the sibling pattern).
3. Red tests (section 10.1/10.2 + the gRPC transport tests with bufconn); the red run
   recorded.
4. Implementation: domain, storage + migration, usecase, transport, main wiring.
5. Green runs: unit + integration + the local e2e smoke; coverage; lint; the em dash
   check.
6. Four review passes; findings closed.
7. AGENTS.md (contracts, architecture, trade-offs, journal, runbook) and the commit
   `CBL-8.1: ...`.
8. Umbrella: the compose env (`NOTIFICATION_SECRET_KEY`), the `.env.example` row, the
   plan/status updates.

## 12. Open questions and decisions

- The merchant REST proxy (`POST /v1/webhook-endpoints`) stays with 7.x/15.4; the stand
  registers through gRPC (recorded). The frozen REST 409 `webhook_endpoint_exists` is
  implemented by the URL unique index (T-NOTIFY-08).
- A secret rotation needs an additive update RPC (cf-3.x); not in 8.1. `secret_key_version`
  is written now, and the operational runbook forbids an ad hoc key change (T-NOTIFY-09).

## 13. Review closure (19.10.2026)

Four review passes on the staged diff; findings closed before the commit:

- the committed stand encryption key default is gone: the compose files require
  `NOTIFICATION_SECRET_KEY` (fail-closed interpolation), the stand value lives in the
  gitignored `deploy/.env` and `.env.example` documents its generation;
- the spec and the migration are reconciled: `events` is JSONB (T-NOTIFY-07), the URL
  unique index implements the frozen 409 (T-NOTIFY-08), `secret_key_version` reserves
  the rotation path (T-NOTIFY-09);
- the request hash uses the normalized event set (duplicates, whitespace and order
  replay), with a regression test;
- the insert conflict resolves to the replay or `webhook_endpoint_exists`;
- the fail-closed startup exits non-zero and the key decoder error is not wrapped (no
  key material in logs); the raw handler error is logged with the request id and the
  merchant id; a success log carries the endpoint id;
- the SSRF guards cover the trailing dot, `*.localhost`, IPv6 zones, CGNAT and 0.0.0.0/8;
  the catalog-sync test pins the domain catalog against the frozen OpenAPI enum; the
  exact boundary values are tested;
- the transport rejects a mismatching `x-merchant-id` metadata element;
- the integration matrix drives the real service over PostgreSQL (register, replay,
  reuse, URL conflict, ciphertext round trip);
- the race test forces the pre-read miss and the insert conflict; the dead domain type
  is removed; the migration indexes follow the sibling naming; the readiness ping is
  bounded;
- the event bound aligns with the frozen REST contract (32);
- the AGENTS.md, `.env.example` and Makefile are updated in the same commit.
