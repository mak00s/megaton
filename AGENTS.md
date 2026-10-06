# megaton

## Scope and Sources

- Read [docs/design.md](docs/design.md) before designing public APIs.
- This package owns the stateful notebook analysis experience and core Google/Sheets operations. Shared multi-step workflows belong in `megaton-app`; client-specific policy belongs in consumer repos.
- API contracts: [docs/api-reference.md](docs/api-reference.md). Recipes: [docs/cookbook.md](docs/cookbook.md). Setup: [README.md](README.md).

## Implementation

- Extend existing `mg` namespaces and Result methods before adding parallel APIs. Keep one canonical public route and preserve documented compatibility.
- Low-level Sheets request builders and retry helpers belong in `megaton/gsheet_lowlevel.py`; worksheet behavior in `megaton/gsheet.py`; the notebook facade in `megaton/start.py`.
- The `mg.sheets` / `mg.sheet` namespaces live in `megaton/services/sheet_namespace.py`; save/append/upsert logic in `megaton/services/sheets_service.py`.
- `ReportResult` / `SearchResult` and their chain methods live in `megaton/_result.py` (re-exported from `megaton/start.py`).
- Keep private helpers private; expose a supported public entry point when consumers need shared functionality.
- Preserve selection state, session defaults, and explicit error behavior. Do not silently replace failures with empty results.

## Verification and Changes

- Run targeted tests for every behavior change; use `python -m pytest --cov=megaton --cov-report=term-missing` for broad changes.
- Update API documentation and [CHANGELOG.md](CHANGELOG.md) with public behavior changes; avoid duplicating full specifications in this file.
- Never commit credentials, tokens, or private notebook output. Do not use live external writes as ordinary unit tests.
- Preserve unrelated worktree changes. Publishing packages, pushing, and releasing require an explicit request.
