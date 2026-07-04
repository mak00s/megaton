# Changelog

Changes since `1.0.0`. For `0.x` history see `docs/changelog-archive.md`.

## 2.0.0 - 2026-07-03

### Removed

- **GA3 / Universal Analytics support removed.** The `megaton/ga3.py` module
  and the `MegatonUA` client are deleted. UA was sunset by Google and the
  support was already deprecated (the `use_ga3=True` deprecation warning
  promised removal in 2.0).
- **`segments` report plumbing removed.** GA4's Data API (`RunReportRequest`)
  has no segment concept, so the `segments=` pass-through in `report.run` and
  the unused `Report.segment` attribute were dead UA-era residue and are gone.
  `mg.report.run(..., segments=...)` now raises `TypeError` (rather than
  silently ignoring it) pointing users to dimension filters / GA4 audiences.
- **`mg.select.sheet()` removed; use `mg.sheets.select()`.** The legacy
  verb-first alias duplicated the canonical `mg.sheets.select(name)` worksheet
  selector. `mg.select` is now purely the interactive picker namespace
  (`mg.select.ga()`).
- **`dates.get_report_range()` removed; use `dates.get_month_window()`.** It
  was a thin compatibility wrapper for a fixed 13-month window
  (`get_month_window(window_months=13)`).
- **`mg.show.ga.properties` renamed to `mg.show.ga.property`** (singular). It
  shows the *current* property's info, not a list, so the plural name was
  misleading and collided with `mg.properties()` (which lists accessible
  properties). Mirrors the `mg.sheets` (all) / `mg.sheet` (current) idiom.

### Changed (breaking)

- `Megaton(...)` no longer accepts the `use_ga3` argument. Passing it now
  raises `TypeError`. Remove `use_ga3=...` from all call sites; GA4 is
  initialized unconditionally.
- `mg.enabled` no longer reports `ga3`; it returns a subset of `ga4`, `gs`, `sc`.
- `mg.ga_ver` now resolves to the single active GA (`'4'`) or `None`; the
  GA3/GA4 tab-switching path is gone.
- **`ipywidgets` is no longer a core dependency.** It moved to a `notebook`
  extra: `pip install megaton[notebook]` for the widget-based auth/selection
  UI. The core install is lighter; headless/programmatic use
  (`Megaton(..., headless=True)`, `for_property`, `for_site`) needs nothing
  extra. In Colab, non-headless `Megaton(...)` auto-installs ipywidgets (same
  mechanism as the GA4 deps) so the picker just works without the extra. The
  widget code paths already raised a clear error when ipywidgets was absent;
  only the packaging default changed.
- **`mg.recipes` replaced by `mg.load.config(url)`.** The deprecated
  `mg.recipes.load_config(url)` accessor is gone; config loading now lives in
  the existing `mg.load` family (alongside `mg.load.csv` / `mg.load.cell`) as
  `mg.load.config(sheet_url)`, returning the same `Config`. Still stateful —
  `mg` holds the sheet connection, so you pass only the URL. The low-level
  `megaton.recipes.load_config(mg, url)` remains available.
- **`mg.report.set_dates()` removed; use `mg.report.set.dates()`.** The flat
  method duplicated the canonical `mg.report.set.dates(...)` namespace (search
  only ever had `mg.search.set.dates()`). One canonical date-setting path now,
  symmetric across report and search. (The low-level `mg.ga["4"].report.set_dates`
  client method is unaffected.)

### Added

- **Unified dimension-filter name across report and search.**
  `mg.search.run(...)` / `mg.search.run.all(...)` now take `filter_d=`, the same
  name report uses (report keeps `filter_d` / `filter_m`). The old
  `dimension_filter=` is a backward-compatible alias; passing both raises
  `TypeError`. (Dimension and metric filters stay separate on purpose — GA4
  requires each in its own request slot, so they are not merged into one
  auto-routed argument.)
- **`mg.sheet.cell.get(cell)`** to read a cell by A1, pairing with
  `mg.sheet.cell.set(...)`.
- **`mg.set.retry(max_retries=, backoff_factor=, timeout=)`** — session-level
  retry config applied across GA4, Sheets, and Search Console, so you set it
  once instead of per call. Resolution order is per-call arg → `mg.set.retry`
  → env (`MEGATON_GS_*` / `MEGATON_GSC_*`) → default. Per-call `max_retries` /
  `backoff_factor` / `timeout` still override (now advanced/escape-hatch).

### Fixed

- **Sheet write retry settings now honor instance configuration.** The
  `mg.save` / `mg.append` / `mg.upsert` / `mg.sheet.*` facades and
  `SheetsService` hardcoded `max_retries=3` / `backoff_factor=2.0`, which
  overrode any instance-level retry config on the way down to
  `_call_with_retry`. These layers now pass `None` and let the single
  resolver decide, so a configured `max_retries` / `backoff_factor` is
  actually used. Default behavior is unchanged (`None` still resolves to
  3 / 2.0).
- **`mg.report.run(...)` now forwards `limit` / `max_retries` /
  `backoff_factor` / `timeout` / `on_exhausted` / `start_date` / `end_date`
  to the GA4 call.** They were silently dropped before reaching
  `MegatonGA4.report.run`, so documented per-call options (e.g. `limit=`) had
  no effect and the defaults (`limit=10000`, etc.) always applied.

### Internal

- **Result objects extracted to `megaton/_result.py`.** `SearchResult`,
  `ReportResult`, `_ResultBase`, `wrap`, and the `KNOWN_GA4_*` sets moved out
  of the ~3900-line `start.py` (now ~2870). Public import paths are unchanged:
  `from megaton.start import SearchResult / ReportResult / wrap` still work
  via re-export.
- **Public API surface declared via `__all__`.** `megaton.start` and
  `megaton._result` now list their stable names
  (`Megaton`, `SearchResult`, `ReportResult`, `MappingRule`, `wrap`).
  Everything else (`_ResultBase`, `_extract_df`, `KNOWN_GA4_*`, service
  classes) is internal and may change without notice.

## 1.5.0 - 2026-07-03

Unified date vocabulary (merged down from megaton-app's megaton_lib date
stack) + deprecation notices. All functional changes are additive.

### Added

- **`dates.resolve_date(expr, *, reference=None, tz=None)`** (strict) — one
  resolver for absolute dates (YYYY-MM-DD / YYYYMMDD), GA-style tokens
  (today / yesterday / NdaysAgo) and calendar tokens (today±Nd,
  month-start/end, year-start/end, week-start, prev-month-start/end,
  prev-prev-month-start/end).
- **`dates.resolve_month(expr)`** — this-month / prev-month /
  prev-prev-month / YYYYMM -> "YYYYMM".
- **Calendar tokens work natively in queries**:
  `mg.report.set.dates("prev-month-start", "prev-month-end")` and
  `mg.search.set.dates(...)` now resolve calendar tokens client-side.
  GA-native tokens (today / NdaysAgo) keep passing through to the API
  unchanged, so existing behavior is untouched.
- **Date-object API**: `today_in_timezone`, `previous_month_window`,
  `month_before_window`, `resolve_period_date`, `resolve_period_month`,
  `previous_month_label`.
- **Month-range / DataFrame helpers**: `month_ranges_for_year`,
  `month_ranges_between`, `months_between`, `previous_month_range`,
  `month_start_months_ago`, `previous_year_start`, `month_suffix_months_ago`,
  `parse_year_month_series`, `drop_current_month_rows`,
  `select_recent_months`, `now_in_tz`.
- **Timezone contract**: new-vocabulary functions resolve tz as
  explicit arg > `MEGATON_TZ` env > Asia/Tokyo (invalid names fall back to
  Asia/Tokyo). Pre-existing functions keep their explicit
  `tz="Asia/Tokyo"` defaults.

### Deprecated

- **`Megaton(use_ga3=True)`** (Universal Analytics support): emits
  `DeprecationWarning`; `ga3.py` will be removed in megaton 2.0.
- **`mg.recipes`** (Sheets config loader): emits `DeprecationWarning` on
  use; will be removed in megaton 2.0 (no known consumers).

## 1.4.3 - 2026-06-12

### Fixed

- **`ReportResult.to_int()` is now robust to object-dtype columns**: uses
  `pd.to_numeric(errors="coerce")` before `astype(int)` (same as
  `transform.fillna_int`), so columns that arrive as object/strings with NaN
  (e.g. GA4 `advertiserAdCost`) convert correctly instead of raising. This
  makes the chain `wrap(df).to_int(cols).group(...)` a true drop-in for the
  legacy `fillna_int(df, cols)` + `groupby(...).sum()` ordering. Verified by
  an all-cells-equal diff on a real shibuya `_ch-m` frame (1141 rows).

## 1.4.2 - 2026-06-12

### Added

- **`ReportResult.group(..., dropna=False, min_count=None)`**: expose pandas
  groupby's `dropna` (keep NaN-key groups) and a `min_count` for sum/prod
  (all-NaN group stays NaN instead of 0). Makes the chain API a drop-in for
  the hand-written `df.groupby(keys, dropna=False)[cols].sum(min_count=1)`
  + `fillna_int` pattern in reports (chain with `.to_int()` for identical
  output).
- **`ReportResult.select(columns, *, strict=True)`**: pick/reorder columns
  (replaces hand-written `df[key_cols]`); updates `dimensions` to the
  surviving subset. `strict=False` skips missing columns.

## 1.4.1 - 2026-06-11

### Fixed

- **GA4 `report.run(timeout=...)`** (default 180s): the per-attempt request
  deadline is now configurable and passed to `run_report`. Previously the
  gRPC client default (~60s) applied, so heavy queries (e.g. 13-month
  windows with `linkUrl` contains-filters) hit client-side
  `DeadlineExceeded` on every retry during slow API periods — retrying could
  never help because each attempt was cut off at the same 60s wall. This was
  initially misdiagnosed as an API outage; light queries were fine all along.

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
- **`megaton.wrap(df, dimensions=None)`**: wrap any DataFrame (BigQuery,
  Sheets, CSV, ...) in a chainable `ReportResult` so all data can use the
  same chain vocabulary (`normalize` / `categorize` / `group` / `to_int` /
  `sort` / `month_key` / ...). Dimensions default to non-numeric columns.
- **Results accepted by save/append/upsert**: `mg.save.to.sheet/csv`,
  `mg.append.to.csv`, `mg.upsert.to.sheet/csv`, and `mg.sheet.save/append/upsert`
  now accept a `ReportResult`/`SearchResult` directly (the underlying
  DataFrame is extracted automatically) — fetch → transform → save in one chain.
- **`ReportResult.month_key(dimension='date', into=None, fmt='%Y-%m')`**:
  standardized month-key derivation from date-like columns.
- **`transform.fillna_int(df, cols)`**: fill NaN + convert to int
  (promoted from megaton-notebooks `lib/pd_utils.py`).
- `megaton.wrap` / `megaton.Megaton` are importable from the package root
  (lazy; `import megaton` stays light).
- **`transform.traffic`**: traffic-source primitives promoted from
  megaton-app `megaton_lib/traffic.py` — `normalize_domain`, `source_host`,
  `is_non_public_dev_source`, `ensure_trailing_slash`,
  `apply_source_normalization` (all exported from `megaton.transform`).

### Changed

- `SearchResult` / `ReportResult` now share `_ResultBase` for
  `normalize` / `categorize` and the value-mapping helpers (internal
  dedup; behavior unchanged).

### Changed (behavior)

- **GA4 retry exhaustion now raises** instead of silently returning an empty
  result. Transient API errors (`ServiceUnavailable` / `DeadlineExceeded` /
  `ResourceExhausted`) are retried with exponential backoff (default attempts
  3 -> 5); when retries are exhausted, the original exception propagates so
  reports fail loudly instead of writing partially-empty data (columns went
  missing silently before). Opt back into the old behavior per call with
  ``report.run(..., on_exhausted='empty')``.

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
