# E2E cbl-notification: scenario plan

Run instruction for local e2e runs of the service. A run is performed by an agent
platform-independently (docker, a command shell, the Go toolchain); run artifacts go to
`test/e2e/.runs/<UTC mark>/`. There are no PowerShell run scripts (customer decision of
16.10.2026, `CBL-002` section 7).

Repository state: the CBL-0.1 scaffold; no scenario is implemented yet. This file is the
plan; scenario sections are filled in with the service tasks (signed merchant webhooks
and retries, CBL-8.x), following the `.specs/templates/E2E.template.md` structure and the
`.additionals/summary.template.md` artifact layout.

## Planned scenarios

| Scenario | Planned checks | Tasks |
| :-- | :-- | :-- |
| `E2E_Notification_MerchantWebhooks` | signed merchant webhook delivery; retry policy with backoff on 5xx and timeouts; idempotency of repeated deliveries; end-to-end flow over the compose stand (Kafka in, HTTP delivery out) | CBL-8.x |

## Conventions for the implemented scenarios

- Run mark: UTC lowercase `yyyymmddthhmmssz`; compose project `cbl-notification-e2e-<mark>`.
- Each scenario starts from a clean stand (`down -v` then `up`), executes its steps with
  all transcripts saved, and ends with `down -v`; mandatory artifacts per `CBL-002`
  section 7 (`summary.md`, `compose-ps.txt`, `compose-up.log`, `compose-down.log`,
  `compose-logs.txt`, `<scenario>.log`, `<scenario>-checks.txt`).
- Ports and environment variables follow `CBL-002` and the service AGENTS.md.

## Auxiliary files

- `summary.template.md` - the `summary.md` run report template;
- `inventory.md` - the list of auxiliary files and their purpose.

`.runs/` is not committed.
