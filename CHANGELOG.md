# Changelog

Changes since `1.0.0`. For `0.x` history see `docs/changelog-archive.md`.

## 1.4.0 - 2026-06-11

Programmatic (script/CI) public API. All changes are additive; no breaking
changes. Goal: scripts and downstream libraries no longer need to reach into
internal attributes (`mg.ga["4"].accounts`, `account.select()`,
`search.get.sites()`).

### Added

- **`Megaton.for_property(property_id, credential=None, *, headless=True)`**:
  classmethod that authenticates and pre-selects a GA4 property in one call.
  Headless by default — safe in scripts and CI.
- **`Megaton.for_site(site_url, credential=None, *, headless=True)`**:
  classmethod that authenticates and pre-selects a Search Console site.
- **`mg.properties(ver=None)`**: flat list of accessible GA properties
  (`{"id", "name", "account_id", "account_name"}`). Replaces iterating
  internal `mg.ga["4"].accounts`.
- **`mg.sites()`**: list of accessible Search Console sites (public wrapper
  of `mg.search.sites`).
- **`mg.use_property(property_id)`**: select account + property by ID.
  Raises `RuntimeError` if GA clients are not initialized, `ValueError`
  (listing accessible IDs) if the property is not accessible.
- **Composite GA4 filters**: `report.run(filter_d=...)` /
  `filter_m=...` now also accept a dict tree for AND/OR/NOT logic, e.g.
  `{"and": ["date==2024-01-01", {"or": ["country==Japan", "country==Taiwan"]}]}`.
  Leaves use the existing legacy string syntax. String filters are unchanged.

### Error contract

Public query APIs raise exceptions from `megaton.errors` (`BadRequest`,
`ApiDisabled`, `BadPermission`, ...). Downstream code is encouraged to catch
these instead of generic `Exception`.

## 1.3.0 - 2026-05-17

### Added

- **`MegatonGS.call_with_retry(op, func, ...)`**: Public helper that runs any
  callable with exponential-backoff retry on transient Google API errors
  (promoted from the private `_call_with_retry`). HTTP 429 quota retries add a
  minimum 30-second wait before the next attempt when the calculated backoff is
  shorter.
- **`MegatonGS.workbook`**: Public read-only property returning the open
  gspread `Spreadsheet` (window onto the internal `_driver`).

### Changed

- Renamed `_call_with_retry` → `call_with_retry`. No backward-compat alias is
  kept; update any caller that referenced the private name.
- Remaining Sheets network reads now retry internally: `MegatonGS.sheets`
  (`worksheets()`) and `Sheet.last_row` (`range()`). `open()`, `select()`,
  `get_records()` were already retry-wrapped — callers no longer need to
  wrap megaton read methods in `call_with_retry` themselves.

### Fixed

- `Sheet.select()` and `save_data(mode="w")` no longer silently swallow
  unrecognized `APIError`s (e.g. HTTP 429 / 5xx). Errors other than
  `disabled` / `PERMISSION_DENIED` are now re-raised instead of being
  dropped, matching `open()`'s behavior.

## 1.2.0 - 2026-05-17

### Added

- **Sheets formatting helpers**: Added `mg.sheet.freeze()`, `mg.sheet.resize()`,
  `mg.sheet.gridlines.hide()`, `mg.sheet.gridlines.show()`, and
  `mg.sheet.tab.color()` for selected worksheet formatting without direct
  gspread calls.

## 1.1.0 - 2026-04-02

### Added

- **GA4 multi-range helper**: `mg.report.run.ranges()` — run `mg.report.run()` over multiple date ranges and concatenate the results.
- **Sheets read shortcut**: `mg.sheets.read()` — select a worksheet and return its data as a `DataFrame` in one step.
- **Sheets duplicate**: `mg.sheets.duplicate(source, new_name, cell_update={"cell": "B1", "value": "..."})` — duplicate a worksheet and optionally patch a single cell in the copy.

### Changed

- **BigQuery API streamlined**: Replaced legacy scheduled-query helpers with a cleaner `bq.dataset.select/update` and `bq.table.select/update/create` API.
- **Dependency cleanup**: Removed `google-cloud-bigquery-datatransfer` dependency.
- **Search Console site URL fallback**: `mg.search.run()` now retries the same query with trailing-slash/no-slash URL-prefix variants when the first `site_url` returns 400/403/404.
- **Search Console retry hardening**: `mg.search.run()` and `list_sites()` now retry on `TimeoutError` / `ConnectionError` / `BrokenPipeError`; retry parameters configurable via `MEGATON_GSC_MAX_RETRIES` / `MEGATON_GSC_BACKOFF_FACTOR` environment variables.
- **Sheets cell retry**: Cell read/write operations (`cell.data`) now use the retry wrapper.
- **Sheets duplicate follow-up behavior**: `mg.sheets.duplicate()` now treats the sheet copy itself as success even if the optional post-duplicate cell update fails; the cell-update failure is reported separately.

## 1.0.0 - 2026-02-07

### Added

- **GA4 report retry**: Added `max_retries` / `backoff_factor` to `mg.report.run()` for exponential backoff on `ServiceUnavailable`.
- **GA4 user properties**: Added `mg.ga["4"].property.show("user_properties")`.
- **Report prep display control**: Added `mg.report.prep(show=False)`.
- **Sheets save start_row**: Added `start_row` to `mg.save.to.sheet()` / `mg.sheet.save()`.
- **Sheets save/append auto-create**: Added `create_if_missing` to `mg.save.to.sheet()` / `mg.append.to.sheet()`.
- **CSV upsert**: Added `mg.upsert.to.csv()`.
- **Search date templates**: `mg.search.set.dates()` now supports `NdaysAgo` / `yesterday` / `today`.

### Changed

- **pyproject.toml migration**: Replaced `setup.py` / `MANIFEST.in` / `requirements.txt` with PEP 621 `pyproject.toml`.
- **Test coverage**: Added branch tests for `sheets_service` / `gsheet` / `ga4 report`.
- **Documentation**: Updated API reference, cheatsheet, and README to match current implementation.
