# megaton

[![PyPI version](https://badge.fury.io/py/megaton.svg)](https://pypi.org/project/megaton/)
[![Python 3.11+](https://img.shields.io/badge/python-3.11%2B-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

Megaton is a Python toolkit for working with Google Analytics 4, Google Search Console, Google Sheets, and BigQuery from Notebooks with minimal code. It focuses on fast iteration during analysis and provides a UX tailored for Notebook workflows.

## Core Concepts

- **Result objects** — Method chaining via `SearchResult` / `ReportResult`
- **Simple flow** — Open → Set dates → Run → Save
- **Notebook-first** — Designed for inspecting intermediate results at every step

## Quick Start

### Prerequisites

You need a Google Cloud **service account JSON** file with access to GA4, Search Console, or Sheets.
See [Google Cloud docs](https://cloud.google.com/iam/docs/creating-managing-service-account-keys) for how to create one.

### Install

```bash
pip install megaton              # core (headless / programmatic use)
pip install megaton[notebook]    # + ipywidgets for the interactive selection UI
```

`ipywidgets` is no longer a core dependency (since 2.0). Install the
`notebook` extra when you want the widget-based credential/account/property
picker used by `Megaton(...)` in Jupyter/Colab. For scripts, CI, or headless
runs, the core install is enough — use `Megaton(..., headless=True)`,
`Megaton.for_property(...)`, or `Megaton.for_site(...)`.

### Run a GA4 report and save to Google Sheets

```python
from megaton.start import Megaton

# Interactive (Jupyter/Colab): needs megaton[notebook] for the picker UI.
mg = Megaton("/path/to/service_account.json")
# Scripts/CI (core install, no widgets): select the property up front.
# mg = Megaton.for_property("YOUR_GA4_PROPERTY_ID", "/path/to/service_account.json")

# GA4: fetch event data
mg.report.set.dates("2024-01-01", "2024-01-31")
result = mg.report.run(d=["date", "eventName"], m=["eventCount"])

# Save to Google Sheets
mg.open.sheet("https://docs.google.com/spreadsheets/d/...")
mg.save.to.sheet("_ga_data", result.df)
mg.sheets.select("_ga_data")
mg.sheet.freeze(rows=1)
mg.sheet.resize(rows=1000, cols=20)
mg.sheet.gridlines.hide()
mg.sheet.tab.color("#2f80ed")
```

### Run the same report over multiple date ranges

```python
df = mg.report.run.ranges(
    date_ranges=[("2024-01-01", "2024-01-31"), ("2025-01-01", "2025-01-31")],
    d=["date", "eventName"],
    m=["eventCount"],
)
```

### Read a worksheet as DataFrame

```python
mg.open.sheet("https://docs.google.com/spreadsheets/d/...")
daily_df = mg.sheets.read("daily")
```

### Duplicate a worksheet and patch a cell

```python
mg.open.sheet("https://docs.google.com/spreadsheets/d/...")
mg.sheets.duplicate(
    "template",
    "report_2024_02",
    cell_update={"cell": "B1", "value": "202402"},
)
```

### Search Console with method chaining

```python
# query_map: dict mapping regex patterns to category names
# e.g. {"brand.*keyword": "Brand", ".*": "(other)"}
result = (mg.search
    .run(dimensions=['query', 'page'], clean=True)
    .categorize('query', by=query_map)
    .filter_impressions(min=100)
)

mg.save.to.sheet('_query', result.df, sort_by='impressions')
```

## Installation

```bash
# From PyPI
pip install megaton

# Latest from GitHub
pip install git+https://github.com/mak00s/megaton.git
```

## Documentation

> **Note:** Detailed docs are written in Japanese.

If you're new, start with the **cookbook** for practical examples, then refer to the **API reference** for details.

| Doc | Description |
|-----|-------------|
| [cookbook.md](docs/cookbook.md) | Practical recipes — start here |
| [api-reference.md](docs/api-reference.md) | Full API reference (single source of truth) |
| [cheatsheet.md](docs/cheatsheet.md) | One-line quick reference |
| [design.md](docs/design.md) | Design philosophy and trade-offs |

## Testing & Coverage

```bash
pytest --cov=megaton --cov-report=term-missing
```

## Changelog

- [CHANGELOG.md](CHANGELOG.md)
- [docs/changelog-archive.md](docs/changelog-archive.md) — 0.x series history

## License

MIT License
