# Failure Handling and Migration

These changes are introduced in 2.2.0. Review this guide when upgrading from 2.1.3
or earlier, especially for unattended scheduled jobs.

## Results and Errors

| Operation | Successful result | Failure |
|---|---|---|
| GA4 / GSC query | DataFrame, possibly empty | API exception propagates |
| Sheets read | DataFrame, possibly empty | Exception; never fabricated empty data |
| Sheets save / append service and `mg.sheet.*` | `True` for a write, `False` for empty input | Exception |
| Sheets upsert | Merged DataFrame; empty input returns existing data without a write | Exception; failed read never leads to a write |

GA4 `on_exhausted='empty'` still explicitly permits an empty first-page result
after transient retry exhaustion. It does not hide permission or invalid-query
errors. A stopped report after a successful page raises `PartialDataReturned`.
GSC URL fallback remains limited to the first page; later failures propagate.

Compatibility open/selection helpers may still return `None` on failure, and
legacy chain wrappers may discard write return values. Always check
`mg.open.sheet(url)` before proceeding. The error contracts above apply to the
data operations, not a wholesale change to all notebook UI helpers.

## Writes and Retry

Stateful overwrite constructs one Sheets `batchUpdate` containing absolute grid
expansion, value clearing, and replacement. It retains existing grid capacity
and formatting. `start_row` preserves earlier rows. Unlike the former full
overwrite, it does not shrink the grid to the DataFrame size. Empty input does
not clear a sheet; use an explicit clear operation when intended.

Timezone-naive datetime columns and ISO dates become numeric Sheets dates.
Fractional seconds are retained within the precision of the floating-point
serial (the default display format shows minutes). Timezone-aware datetime
columns remain ISO text including their offset; no implicit timezone conversion
is performed. Numeric-looking strings remain text. Date number formats apply
only to date cells, so text and blank cells in mixed columns keep their formats.

The batch is atomic, but a timeout does not prove it failed: the server may
have applied the complete batch. Absolute replacement retries use the same
payload. Worksheet create/delete/duplicate and structural row growth are sent
once. After an uncertain structural write, inspect the destination before
re-running it. An atomic overwrite does not make concurrent read/merge/write
upserts safe: serialize writers to the same worksheet.

Column width and freeze options run after the data write. Failure there may
leave data updated with incomplete presentation options; it does not roll back
the data. Workbook-wide multi-tab transactions are not implied.

## CLI Diagnostics

Callers can serialize exception context without converting failure into success:

```python
import json
from megaton.errors import describe_error

try:
    mg.sheet.upsert(df, keys=["date", "page"])
except Exception as exc:
    print(json.dumps(describe_error(
        exc, operation="sheet.upsert", target="report-book/daily"
    ), ensure_ascii=False))
    raise
```

The result includes `error_type`, `message`, `cause_type`, `cause_message`,
`http_status`, and `transient`. Unknown status is `None`. The transient flag
classifies HTTP status only and is not permission to replay a mutation. Use
non-secret target labels and avoid logging credentials or private query data.

## OAuth Cache Migration

Cache names include a hash of the canonical credential path. Different folders
with the same JSON filename no longer share tokens. New cache files have mode
0600. Existing basename-only caches are left untouched and are not loaded:
reauthorize once to create a path-specific cache. Moving a credential file also
requires reauthorization. Keep separate credential paths for separate accounts.

## Internal Ownership

`start.py` retains session orchestration and public entry points. The Sheets
namespaces live in `services/sheet_namespace.py`; business operations live in
`services/sheets_service.py`; `gsheet.py` handles the stateful client and
`gsheet_lowlevel.py` owns shared request/value builders and retry primitives.
Add new reusable behavior at the service or low-level boundary rather than
duplicating it inside the session namespace.
