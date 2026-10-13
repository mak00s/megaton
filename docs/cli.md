# Single-Query Python and CLI Contract

Available from Megaton 2.3.0. Python, CLI and MCP adapters use
`megaton.query.run_query`; Notebook `mg.*` remains supported.

## Install and Run

```bash
pip install megaton
# BigQuery DataFrame conversion also needs db-dtypes:
pip install 'megaton[bigquery]'
megaton query --params query.json --credential /absolute/path/google.json
megaton query --params query.json --credential /absolute/path/google.json --output result.csv
megaton query --params query.json --validate-only
cat query.json | python -m megaton query --params - --validate-only
megaton resources ga4 --credential /absolute/path/google.json
megaton resources gsc --credential /absolute/path/google.json
megaton resources bigquery --project-id example-project --adc
```

No app checkout, credential-directory scanning or client inventory is required.
Explicit credential files use existing Megaton authentication for GA4/GSC.
User OAuth requires an existing cache; pass `--cache-key` where needed.
GA4 currently uses Analytics Admin API property discovery as well as Data API.
BQ credential files use Google Auth; ADC is available only by explicit `--adc`.
The resources command lists properties, sites or datasets, not client aliases.

## Request Version 1.0

All requests require `schema_version: "1.0"` and `source`.
Unknown fields fail before authentication. Absolute ISO dates are required;
relative-date templates and site aliases remain app features.

| Source | Required fields | Optional fields |
| --- | --- | --- |
| `ga4` | `property_id`, `date_range`, `dimensions`, `metrics` | `limit`, `filter_d`, `filter_m`, `sort` |
| `gsc` | `site_url`, `date_range`, `dimensions` | `limit`, `filter_d`, `page_to_path` |
| `bigquery` (`bq` alias) | `project_id`, `sql` | `location` |

`date_range` has exactly `start` and `end`, formatted YYYY-MM-DD, start <= end.
GA4 fields are strings or `[api_name, output_alias]` pairs. GSC dimensions are
`date`, `hour`, `country`, `device`, `page`, `query`, `searchAppearance`, and
the client-side `month` aggregation, with no aliases. `searchAppearance` also
works in filter objects. See the [official Search Analytics API](https://developers.google.com/webmaster-tools/v1/searchanalytics/query).
When grouping by `searchAppearance`, it must be the only dimension. For a
breakdown, first list appearances, then filter by one appearance and group by
`query`, `page`, or other dimensions. See the [official two-step workflow](https://developers.google.com/webmaster-tools/v1/how-tos/all-your-data#getting_search_appearance_data).
`limit` defaults to 1000 and accepts
integers 1..100000: it caps the number of source rows fetched, including the
last page. Results are not necessarily complete when the cap is reached.
Notebook `mg.report.run(limit=...)` retains its page-size meaning; the new
single-query API passes a separate `max_rows` to enforce its total cap.
GA4 permits no dimensions for metric totals. GSC defaults to full page URLs.

GA4 filters accept a condition string or an `and`/`or`/`not` dict tree; plain
lists are rejected. `sort` accepts a comma-separated string or a list of field
names, prefixed with `-` for descending or `+` for ascending. Output aliases
are resolved to API fields. GSC filters accept a condition string or a list of
dimension/operator/expression objects. `--validate-only` checks structural
executability and authentication-option conflicts; resource access, GA4 metadata
field existence and provider validation still require execution.

With `page_to_path`, GSC collapses full URLs to paths and reaggregates by all
requested dimensions. Clicks/impressions are summed, CTR is clicks/impressions,
and position is weighted by impressions. The row cap applies before aggregation.

```json
{
  "schema_version": "1.0", "source": "gsc",
  "site_url": "sc-domain:example.com",
  "date_range": {"start": "2026-01-01", "end": "2026-01-31"},
  "dimensions": ["query", "page"],
  "filter_d": [{"dimension": "query", "operator": "contains", "expression": "example"}],
  "limit": 25000, "page_to_path": false
}
```

```json
{
  "schema_version": "1.0", "source": "bigquery",
  "project_id": "example-project", "location": "US",
  "sql": "SELECT 1 AS value"
}
```

BQ executes the supplied SQL and can incur charges or mutate data. The CLI is
not a read-only sandbox. SQL parameters and job orchestration remain in app.

## Result and Failure

Python returns `QueryResult(df, params)`. A selected Megaton session can be
passed as `client=` for GA4/GSC; for BQ it is a native BigQuery client.
The session's target and dates are explicitly updated. Do not concurrently
share one mutable session between executions. Authentication arguments cannot
be combined with `client=`. Resource discovery creates its own client.

Without `--output`, stdout contains one JSON document with `status`,
`schema_version`, `source`, `row_count`, `columns`, normalized `params` and
`data`. Dates use ISO JSON values; nonfinite/missing values become null.
CSV output has headers and no DataFrame index. JSON files use the result
envelope. With `--output`, stdout reports path and row count instead of rows.
Requests/SQL/results may contain sensitive data: callers own storage policy.

Exit 0 means success (including zero rows), 2 means invalid request/options, 1 means
execution/file/authentication failure. Errors are JSON on stdout. Provider
messages are omitted from execution errors; `error_type` is retained. Python
exceptions preserve the provider cause for trusted debugging. Legacy library
prints are redirected to stderr, so callers should treat stderr as potentially
sensitive diagnostics. No credentials are included in result metadata.

## MCP

An MCP server imports `run_query` and converts `result.to_dict()` to its tool
response. It configures authentication server-side, limits accessible resources
and SQL, and decides which writes require approval. No MCP transport dependency
is required by Megaton. Operational status, report quality and delivery tools
remain in the private operational MCP layer.
