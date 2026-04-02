# Changelog

Changes since `1.0.0`. For `0.x` history see `docs/changelog-archive.md`.

## 1.1.0 - Unreleased

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
