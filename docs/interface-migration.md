# Python / CLI / MCP Boundary Migration

Status: implementation and releases complete (2026-10-13).

## Contract

Megaton owns reusable Google analytics operations and single-query execution.
Python, installed CLI, Notebook and MCP adapters share those operations.
Notebook state remains convenient; machine requests explicitly select source,
resource, dates and authentication. Megaton does not require megaton-app.

megaton-app is public again following repository sanitization. It owns workflow
orchestration, credential routing, Adobe integrations, batch/jobs, audit,
validation and delivery. Consumers own client inventory and business policy.
MCP adapters own transport, access control and tool exposure, not query logic.

## Checklist

- [x] Inspect current Megaton 2.2.0 and clean megaton-app 0.39.0.
- [x] Clone clean app separately; old checkouts remain push-disabled.
- [x] Add validated single-query Python API and result contract.
- [x] Add installed CLI and resource discovery.
- [x] Delegate app GA4/GSC execution while preserving credential routing and DataFrame output.
- [x] Update design, architecture and user documentation.
- [x] Verify unit/contracts, full suites and installed-wheel execution.

## First Release Scope

Sources: GA4, GSC and BigQuery. Explicit credential file or explicitly opted-in
ADC for BigQuery. No directory scanning or checkout-relative configuration.
GA4/GSC may also reuse an explicitly selected Megaton instance. Their standalone
entry uses existing headless authentication; GA4 property discovery still uses
Analytics Admin API. Removing that requirement is a separate auth improvement.

The app's JSON contract additionally owns aliases, relative dates, pipelines,
save policies and AA. Do not move its entire validator or query CLI wholesale.
BigQuery SQL can mutate data and incur charges; execution is explicit and is
not advertised as read-only. MCP exposure must enforce its own allowed actions.

## Verification

- Initial implementation: 654 passed. Review fixes including searchAppearance:
  full suite 683 passed; 29 real-execution regression tests plus original contracts.
- App GSC/GA4 integration follow-up: 57 targeted tests passed. Its earlier full
  run was 1438 passed; the follow-up did not rerun the entire app suite.
- Core coverage after review fixes: 74.23%, above the existing 70% gate.
- App full suite: 1438 passed; existing three dependency/deprecation warnings.
- App query CLI coverage: 98%; shared-library coverage gate (64%) passed.
- Blocking Pyright: zero errors in both repos, including new core modules.
- Ruff: new core modules/tests and app library passed.
- Core 2.3.0 and app 0.40.0 wheel/sdist builds and Twine checks passed.
- App wheel/sdist boundary check passed: no checkout-local content.
- Installed core CLI and python-module CLI validated requests from /tmp without
  app or ipywidgets installed. Subsequently installed app wheel and exercised
  app-to-core GA4 delegation with a mocked API client from /tmp.
- App full-history keyed identifier and secret-pattern checks passed in GitHub
  CI (run 38382322577). Local key remains unavailable; CI supplied the Secret.
- Live Google calls were not performed. Tests mock API responses and verify
  target selection, dates, filters, errors, empty results and output contracts.

## Released Versions and Follow-up

1. Megaton 2.3.0: main CI passed on Python 3.11/3.12/3.13, tag pushed,
   GitHub release and PyPI wheel/sdist published. Fresh PyPI install and CLI passed.
2. App 0.40.0: pushed after the core dependency was available; main CI passed
   including keyed hygiene, typing, tests, coverage and packaging. Tag and GitHub
   release published with wheel/sdist. No app PyPI publication.
3. Consumer pins remain unchanged. megaton-notebooks stays on app v0.33.0.
   Upgrade it separately after reviewing row caps and comparing old/new totals.

- [Core release](https://github.com/mak00s/megaton/releases/tag/v2.3.0)
- [Core PyPI](https://pypi.org/project/megaton/2.3.0/)
- [Core CI](https://github.com/mak00s/megaton/actions/runs/38381918490)
- [Core publisher](https://github.com/mak00s/megaton/actions/runs/38382116917)
- [App release](https://github.com/mak00s/megaton-app/releases/tag/v0.40.0)
- [App main CI](https://github.com/mak00s/megaton-app/actions/runs/38382322577)

Reviewed changes and release preparation are committed and pushed. Core release
precedes app release. Consumer pins stay fixed.
Core work: /Users/mak/Repos/megaton.
App work: clean megaton-app-current under this task's workspace. Old app
checkouts remain diagnostic-only with their existing push-disabled settings.

Future work: remove GA4 Admin-discovery dependency from explicit single queries,
extend BQ parameter contracts when needed, and add a separately deployed MCP
query adapter. Existing operational MCP remains responsible for pipeline status,
quality, completion and delivery rather than implementing analytics queries.

## Review Fixes (2026-10-13)

- [x] Reused GA4 sessions cannot return previous rows after BadRequest: clear
  report.data and propagate failures from the real Notebook execution layer.
- [x] Sort accepts string/list names and resolves output aliases to API fields.
- [x] Single-query limit caps source rows, not page size; GA4 max_rows and GSC
  final-page sizing stop pagination at the cap. Notebook GA4 page size remains.
- [x] Validate supported GA4 filter trees, reject plain lists; validate GSC
  filter objects/dimension names and reject GSC aliases before authentication.
- [x] Path conversion reaggregates clicks/impressions, recomputes CTR and
  impression-weighted position, retaining other dimension partitions.
- [x] Failed GA4 resource discovery raises instead of reporting empty success.
- [x] Standalone GSC authentication explicitly skips GA4 initialization.
- [x] Authentication failure is an execution exception (CLI exit 1).
- [x] Authentication option checks are shared by query/resources/validate-only.
- [x] Follow-up: support `searchAppearance` in both GSC validation and execution,
  including filters and validate-only CLI. Dimension inventory now has one owner.
- [x] Enforce the officially documented standalone searchAppearance grouping
  restriction in both layers. Appearance filters can still accompany other
  grouping dimensions, following Google's two-step workflow.

Tests use actual Megaton.Report / MegatonGA4.Report / GSCService execution with
stubbed network responses, not just report.run mocks. Updated app changelog
documents row-cap and GSC alias behavior changes for existing consumers.
