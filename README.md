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
pip install megaton
```

### Run a GA4 report and save to Google Sheets

```python
from megaton.start import Megaton

mg = Megaton("/path/to/service_account.json")

# GA4: fetch event data
mg.report.set.dates("2024-01-01", "2024-01-31")
result = mg.report.run(d=["date", "eventName"], m=["eventCount"])

# Save to Google Sheets
mg.open.sheet("https://docs.google.com/spreadsheets/d/...")
mg.save.to.sheet("_ga_data", result.df)
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

## Changelog

- [CHANGELOG.md](CHANGELOG.md)
- [docs/changelog-archive.md](docs/changelog-archive.md) — 0.x series history

## License

MIT License
