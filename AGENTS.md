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
  RegisterWebhookEndpoint - endpoint registration and secret issuance)
- Kafka produces: none
- Kafka consumes: payment.events, payout.events, recon.alerts
- Storage: PostgreSQL notification
- Merchant delivery: contract `openapi/webhooks.v1.yaml` (envelope, type
  catalog, signature, retries; frozen CF-3)

Contracts are frozen in `cbl-protos` (CF-1 for gRPC, CF-2/CF-3 for events and
webhooks), see `CBL-004`.

## 3. Architecture

Layers: `internal/transport` -> `internal/usecase` -> `internal/domain` -> `internal/storage`.
Libraries: the standard `CBL-002` set (Gin, GORM + pgx driver, franz-go + pkg/sr, go-redis,
goose, zap). `AutoMigrate` is forbidden, migrations are goose only, forward only.

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
Variables (applicable to the service): `HTTP_ADDR`, `GRPC_ADDR`, `METRICS_ADDR`, `PG_DSN`,
`KAFKA_BROKERS`, `SCHEMA_REGISTRY_URL`, `REDIS_ADDR`.

## 6. Development rules

- Pipeline: SDD spec in `.specs/` -> architecture -> contracts -> tests first -> code ->
  review by 4 agents (no-context, architecture, with-context, security - blocking) ->
  AGENTS.md -> final runs -> linters -> commit.
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

## 9. Known issues

| ID | Problem | Impact | Plan | Status |
| :-- | :-- | :-- | :-- | :-- |
| KI-notification-01 | CI and pre-commit are not configured | linters run manually | task 0.2 | open |

## 10. Task journal

| CBL | Date | Status | Spec | Commit |
| :-- | :-- | :-- | :-- | :-- |
| CBL-16.12 | 2026-10-16 | done (English comments and documentation; e2e run instruction `test/e2e/E2E.md` added as a plan, scenarios arrive with CBL-8.x; no code changes) | `.specs/CBL-16.12-conventions.md` | this commit |

## 11. Runbook

To be filled by the first product task of the service. Typical incidents: dependency
unavailability, error growth, processing lag; actions: check health/metrics, logs with
request_id, the state of the Kafka consumer group and outbox lag, restart the service if needed.
