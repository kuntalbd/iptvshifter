# Documentation Index & Update Workflow

> **Purpose:** This is the **entry point** for all documentation. When a
> requirement, feature, or behavior changes, consult this index to find which
> document to update so the docs stay aligned. All docs are **live documents** —
> update them on every change, not just at release.

## Document Map

| Document | Role | Source of truth for | Update when… |
|----------|------|---------------------|--------------|
| `requirement/requirement_overview_v1.2.md` | **Requirements spec** (§1–§25) | *What* the system must do (business rules, modes, blacklist tiers, output formats, schema) | A feature's business rule changes; bump to v1.3 and note supersession |
| `requirement/qa_test_plan_v1.md` | **Unit/integration test plan** (P1–P10, B0–B8) | How each requirement is verified by automated tests (81 tests) | A new requirement phase is added; a test is added/changed |
| `docs/ARCHITECTURE.md` | **Architecture & ADRs** | Component contracts, data model, design decisions (ADR-001..014) | A module boundary, ADR, or data-model column changes |
| `docs/END_TO_END_TESTING.md` | **Full project-wide E2E** | Feature inventory + business logic + E2E test matrix (TC-CLI/UI/API/INT/FAIL) + execution | A feature is added/changed; a new E2E flow appears; UI route/API changes |
| `docs/CONFIGURATION.md` | **Config reference** | Every `config.yaml` key, env var, default | A config key is added/renamed/removed |
| `docs/INSTALL_AND_USAGE.md` | **Install & usage** | How to install, every CLI command, Docker | Install steps or CLI surface change |
| `docs/PROJECT_STRUCTURE.md` | **Source layout** | File/dir tree + responsibility | New module added/removed |
| `README.md` (root) | **Overview** | One-page intro + doc links | Doc filenames change; new top-level feature |
| `prod/AUTONOMOUS_DECISIONS.md` | **Autonomous run log** | Decisions made without live confirmation (review/revert) | An autonomous run makes a non-trivial choice |

## Update Workflow (align after every change)

When a change lands (PR, autonomous run, hotfix), apply the matching row:

1. **Requirement/business-rule change**
   → update `requirement_overview_v1.2.md` (new version + supersession note)
   → add/adjust phase in `qa_test_plan_v1.md`
   → reflect in `END_TO_END_TESTING.md` Part A + add TC in Part C
   → if architecture affected, `ARCHITECTURE.md` ADR

2. **New feature / UI route / API**
   → `END_TO_END_TESTING.md` Part A (feature inventory) + Part C (TC-UI/API/INT)
   → `ARCHITECTURE.md` if new component
   → `INSTALL_AND_USAGE.md` if user-facing command
   → `PROJECT_STRUCTURE.md` if new module

3. **Config key change**
   → `CONFIGURATION.md`
   → `requirement_overview` §12 if behavioral
   → `END_TO_END_TESTING.md` Part A.3/A.12 if mode-affecting

4. **Bug fix / behavior change (no spec change)**
   → `END_TO_END_TESTING.md` Part E (change log) + add TC-FAIL if regression-prone
   → `ARCHITECTURE.md` ADR if design-level

5. **Test added**
   → `qa_test_plan_v1.md` phase table
   → `END_TO_END_TESTING.md` matrix if it's an E2E flow

## Notes
- **CODE-DRIFT:** Where running code differs from `requirement_overview_v1.2.md`,
  `END_TO_END_TESTING.md` marks it `[CODE-DRIFT]` and states the actual behavior.
  Resolve drift by updating the requirement (preferred) or the code.
- **Single source per concern:** don't duplicate requirement text in ARCHITECTURE;
  cross-reference instead.
- **Verification:** after doc changes, run `hermes verify` to keep tests green and
  the matrix honest.
