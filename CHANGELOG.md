# Changelog

## 0.7.0
- Search Console: shorter flow via `mg.search` (`run(...)`, `sites`, `get.sites()`)
- Date ranges: easier month windows with `mg.report.set.months(ago=...)` and `mg.report.window`
- Sheets UX: `mg.sheets` / `mg.sheet` plus `save/append/upsert` verbs for notebooks
- Report helper: `mg.report.dates.to.sheet(...)` to write period cells quickly

## 0.6.0
- refactor: auth/provider separation with compatibility preserved
- refactor: Sheets/BigQuery service extraction
- ui: lazy import for ipywidgets (headless-friendly)
- ci: GitHub Actions pytest workflow (fast, no external calls)
- docs: README/advanced updates and smoke notebook guidance

This release focuses on internal refactoring and stability improvements.
Public APIs remain backward-compatible.
