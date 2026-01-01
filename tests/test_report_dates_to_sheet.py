from types import SimpleNamespace

import pandas as pd
import pytest

from megaton.start import Megaton


def _make_app_with_ga():
    app = Megaton(None, headless=True)
    app.ga = {"4": SimpleNamespace(report=SimpleNamespace(start_date=None, end_date=None))}
    app.state.gs_url = "https://example.com/sheet"
    return app


def test_report_dates_to_sheet_updates_cells(monkeypatch):
    app = _make_app_with_ga()
    app.report.set_dates("2024-01-01", "2024-01-31")

    called = {}

    def fake_update_cells(sheet_url, sheet_name, updates):
        called["sheet_url"] = sheet_url
        called["sheet_name"] = sheet_name
        called["updates"] = updates
        return True

    monkeypatch.setattr(app.sheets_service, "update_cells", fake_update_cells)

    result = app.report.dates.to.sheet("Period", "B2", "B3")

    assert result is True
    assert called["sheet_url"] == "https://example.com/sheet"
    assert called["sheet_name"] == "Period"
    assert called["updates"] == {"B2": "2024-01-01", "B3": "2024-01-31"}


def test_report_dates_str_is_empty_when_unset():
    app = _make_app_with_ga()
    assert str(app.report.dates) == ""


def test_upsert_to_sheet_calls_service(monkeypatch):
    app = Megaton(None, headless=True)
    app.state.gs_url = "https://example.com/sheet"
    df = pd.DataFrame([{"a": 1}])

    called = {}

    def fake_upsert_df(sheet_url, sheet_name, df_new, keys, columns=None, sort_by=None, create_if_missing=True):
        called["sheet_url"] = sheet_url
        called["sheet_name"] = sheet_name
        called["df_new"] = df_new
        called["keys"] = keys
        called["columns"] = columns
        called["sort_by"] = sort_by
        called["create_if_missing"] = create_if_missing
        return "ok"

    monkeypatch.setattr(app.sheets_service, "upsert_df", fake_upsert_df)

    result = app.upsert.to.sheet("Report", df, keys=["a"], columns=["a"], sort_by=["a"])

    assert result == "ok"
    assert called["sheet_url"] == "https://example.com/sheet"
    assert called["sheet_name"] == "Report"
    assert called["df_new"] is df
    assert called["keys"] == ["a"]
    assert called["columns"] == ["a"]
    assert called["sort_by"] == ["a"]
    assert called["create_if_missing"] is True


def test_upsert_to_sheet_rejects_invalid_df():
    app = Megaton(None, headless=True)
    app.state.gs_url = "https://example.com/sheet"

    with pytest.raises(TypeError, match="pandas DataFrame"):
        app.upsert.to.sheet("Report", "not-a-df", keys=["a"])
