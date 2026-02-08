# megaton

Megaton is a Python toolkit for working with Google Analytics 4, Google Search Console, Google Sheets, and BigQuery from Notebooks with minimal code. It focuses on fast iteration during analysis and provides a UX tailored for Notebook workflows.

## Core Concepts

- **Result objects**: Method chaining via `SearchResult` / `ReportResult`
- **Simple flow**: Open → Set dates → Run → Save
- **Notebook-first**: Designed for inspecting intermediate results

## Quick Start

```python
from megaton.start import Megaton

mg = Megaton("/path/to/service_account.json")
mg.report.set.dates("2024-01-01", "2024-01-31")
mg.report.run(d=["date", "eventName"], m=["eventCount"])

mg.open.sheet("https://docs.google.com/spreadsheets/d/...")
mg.save.to.sheet("_ga_data", mg.report.data)
```

## More Practical Example

```python
# Fetch Search Console data for multiple sites and categorize
result = (mg.search.run.all(
    sites,
    dimensions=['query', 'page'],
    item_key='clinic',
)
    .categorize('query', by=query_map)
    .categorize('page', by=page_map))

mg.save.to.sheet('_query', result.df, sort_by='impressions')
mg.upsert.to.csv(result.df, filename='query_master', keys=['clinic', 'query', 'page'], include_dates=False)
```

## Installation

```bash
# From PyPI
pip install megaton

# Latest from GitHub
pip install git+https://github.com/mak00s/megaton.git
```

## Documentation

- [api-reference.md](docs/api-reference.md) - API reference (single source of truth)
- [design.md](docs/design.md) - Design philosophy and trade-offs
- [cookbook.md](docs/cookbook.md) - Practical recipes
- [cheatsheet.md](docs/cheatsheet.md) - One-line reference

## Changelog

- [CHANGELOG.md](CHANGELOG.md)
- [docs/changelog-archive.md](docs/changelog-archive.md) - 0.x series history

## License

MIT License
