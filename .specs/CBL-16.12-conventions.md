# CBL-16.12. cbl-notification: English comments and documentation; e2e via E2E.md

| Field | Value |
| :-- | :-- |
| Epic | E16, `Convention alignments` |
| Repository | `cbl-notification` |
| Size | XS |
| Wave and parallel group | W0-CORR |
| Depends on | `CBL-16.1` (convention), `CBL-16.2` (pilot pattern) |
| Contract version | not applicable (no contract changes) |
| Status | done, review closed |

## 1. Context and goal

Two cross-cutting customer corrections (16.10.2026): (1) comments and documentation are
written in English in all files; (2) e2e runs are not PowerShell scripts but an agent
instruction `test/e2e/E2E.md` with run artifacts in `test/e2e/.runs/` (`CBL-002` section
7). This repository is the CBL-0.1 scaffold; no scenario is implemented yet.

## 2. Scope

In scope: English translation of all Russian text present (AGENTS.md, README.md,
cmd/notification/main.go comments, internal package docs); `test/e2e/E2E.md` as the
scenario plan (scenarios arrive with CBL-8.x); `.additionals/` (summary template,
inventory); `.gitignore` (`.runs/`).

Out of scope: business code (does not exist yet), runtime log literals, other
repositories (16.15/16.16).

## 3. Requirements

FR-1. Language: no Russian comments or docstrings remain; godoc docstrings keep the
entity-name first line and end with a period; AGENTS.md development rules state the
English documentation rule.

FR-2. E2E: `test/e2e/E2E.md` documents the planned scenarios and the run conventions
(lowercase UTC mark, compose project per mark, clean-stand rule, mandatory artifacts);
there are no PowerShell run scripts; AGENTS.md references the instruction.

FR-3. `.runs/` is gitignored.

FR-4. Validation: build, vet, tests and lint green (no scenarios to run yet; the first
scenario lands with CBL-8.x).

## 4. Plan

Translation -> E2E.md plan and `.additionals` -> green battery -> commit
`CBL-16.12: English comments and docs; e2e plan via E2E.md`.

## 5. Reviews and runs

Verified: build and vet green, lint 0 (the `.golangci.yml` was converted from the v1
format to v2 with the same linter set so the pinned latest golangci-lint image accepts
it, related to KI-07), em-dash check clean, Cyrillic scan clean (no residual Cyrillic in
the repository).
