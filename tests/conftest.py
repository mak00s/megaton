import sys
from pathlib import Path

# Ensure tests import the local working tree, not an installed `megaton`.
ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))



import pytest


@pytest.fixture(autouse=True)
def _quiet_store_hints(monkeypatch):
    """Keep credential-store tips out of unrelated tests; hint tests re-enable them."""
    from megaton.auth import hints

    monkeypatch.setenv(hints.HINTS_ENV, "0")
    monkeypatch.setattr(hints, "_shown", set())
