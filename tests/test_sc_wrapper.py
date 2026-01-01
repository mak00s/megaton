from datetime import datetime
from types import SimpleNamespace
from zoneinfo import ZoneInfo

import pytest

from megaton import dates
from megaton.start import Megaton


def test_sc_fetch_sites_updates_state(monkeypatch):
    app = Megaton(None, headless=True)
    monkeypatch.setattr(app._gsc_service, "list_sites", lambda: ["https://example.com"])

    result = app.sc.fetch.sites()

    assert result == ["https://example.com"]
    assert app.sc.sites == ["https://example.com"]


def test_sc_query_uses_report_dates_and_limit(monkeypatch):
    app = Megaton(None, headless=True)
    app.ga = {
        "4": SimpleNamespace(report=SimpleNamespace(start_date="2024-01-01", end_date="2024-01-31"))
    }
    app.sc.use("https://example.com")

    called = {}

    def fake_query(**kwargs):
        called["kwargs"] = kwargs
        return "ok"

    monkeypatch.setattr(app._gsc_service, "query", fake_query)

    result = app.sc.query(dimensions=["page"], limit=123)

    assert result == "ok"
    assert called["kwargs"]["site_url"] == "https://example.com"
    assert called["kwargs"]["start_date"] == "2024-01-01"
    assert called["kwargs"]["end_date"] == "2024-01-31"
    assert called["kwargs"]["row_limit"] == 123


def test_sc_set_months_sets_dates(monkeypatch):
    app = Megaton(None, headless=True)
    app.sc.use("https://example.com")

    now = datetime(2025, 3, 15, 10, 0, 0, tzinfo=ZoneInfo("Asia/Tokyo"))
    expected = dates.get_month_window(months_ago=1, window_months=1, tz="Asia/Tokyo", now=now)

    result = app.sc.set.months(ago=1, window_months=1, tz="Asia/Tokyo", now=now)

    assert result == expected
    assert app.sc.start_date == expected[0]
    assert app.sc.end_date == expected[1]
    assert app.sc.last_month_window["ym"] == expected[2]


def test_sc_query_requires_site():
    app = Megaton(None, headless=True)
    with pytest.raises(ValueError, match="site is not set"):
        app.sc.query(dimensions=["page"])
