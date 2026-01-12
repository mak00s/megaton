from types import SimpleNamespace

import pandas as pd
import pytest

from megaton.start import Megaton


class _FakeSheet:
    def __init__(self):
        self._name = None
        self._data = {"config": [{"a": 1}]}
        self.deleted = []

    def select(self, name):
        self._name = name
        return name

    def create(self, name):
        self._name = name

    def delete(self, name):
        self.deleted.append(name)
        if name in self._data:
            del self._data[name]
        if self._name == name:
            self._name = None

    def clear(self):
        self._data[self._name] = []

    @property
    def name(self):
        return self._name

    @property
    def data(self):
        return self._data.get(self._name, [])


class _FakeGS:
    def __init__(self):
        self.sheet = _FakeSheet()
        self._title = "Fake Sheet"
        self._url = "https://example.com/sheet"
        self._sheets = ["config", "CV"]

    @property
    def title(self):
        return self._title

    @property
    def url(self):
        return self._url

    @property
    def sheets(self):
        return list(self._sheets)


def _make_app_with_gs():
    app = Megaton(None, headless=True)
    app.gs = _FakeGS()
    app.state.gs_url = app.gs.url
    return app


def test_legacy_gs_client_still_available():
    app = _make_app_with_gs()
    assert app.gs.title == "Fake Sheet"
    assert "config" in app.gs.sheets
    assert app.gs.url == "https://example.com/sheet"
    assert app.gs.sheet.select("config") == "config"


def test_sheets_select_sets_state():
    app = _make_app_with_gs()
    app.sheets.select("CV")
    assert app.state.gs_sheet_name == "CV"


def test_sheet_cell_and_range_use_selected_sheet(monkeypatch):
    app = _make_app_with_gs()
    app.state.gs_sheet_name = "CV"

    called = {}

    def fake_update_cells(sheet_url, sheet_name, updates):
        called["cells"] = (sheet_url, sheet_name, updates)
        return True

    def fake_update_range(sheet_url, sheet_name, a1_range, values):
        called["range"] = (sheet_url, sheet_name, a1_range, values)
        return True

    monkeypatch.setattr(app._sheets, "update_cells", fake_update_cells)
    monkeypatch.setattr(app._sheets, "update_range", fake_update_range)

    app.sheet.cell.set("L1", "2024-01-01")
    app.sheet.range.set("L1:N1", [["2024-01-01", "2024-01-31"]])

    assert called["cells"] == (
        "https://example.com/sheet",
        "CV",
        {"L1": "2024-01-01"},
    )
    assert called["range"] == (
        "https://example.com/sheet",
        "CV",
        "L1:N1",
        [["2024-01-01", "2024-01-31"]],
    )


def test_sheet_save_append_upsert_use_current_sheet(monkeypatch):
    app = _make_app_with_gs()
    app.state.gs_sheet_name = "CV"
    df = pd.DataFrame([{"a": 1}])

    called = {}

    def fake_save(sheet_name, df_in, **kwargs):
        called["save"] = (sheet_name, df_in, kwargs)

    def fake_append(sheet_name, df_in, **kwargs):
        called["append"] = (sheet_name, df_in, kwargs)

    def fake_upsert(sheet_url, sheet_name, df_in, keys, columns=None, sort_by=None, create_if_missing=True):
        called["upsert"] = (sheet_url, sheet_name, df_in, keys, columns, sort_by, create_if_missing)
        return "ok"

    monkeypatch.setattr(app._sheets, "save_sheet", fake_save)
    monkeypatch.setattr(app._sheets, "append_sheet", fake_append)
    monkeypatch.setattr(app._sheets, "upsert_df", fake_upsert)

    app.sheet.save(df)
    app.sheet.append(df)
    result = app.sheet.upsert(df, keys=["a"])

    assert called["save"][:2] == ("CV", df)
    assert called["save"][2] == {
        "sort_by": None,
        "sort_desc": True,
        "auto_width": False,
        "freeze_header": False,
    }
    assert called["append"] == ("CV", df, {})
    assert result == "ok"
    assert called["upsert"][:3] == ("https://example.com/sheet", "CV", df)


def test_sheet_requires_spreadsheet_and_selection():
    app = Megaton(None, headless=True)

    with pytest.raises(ValueError, match="active spreadsheet"):
        app.sheets.select("CV")

    app.gs = _FakeGS()
    app.state.gs_url = app.gs.url
    with pytest.raises(ValueError, match="worksheet selected"):
        app.sheet.save(pd.DataFrame())


def test_sheets_delete_clears_state_and_calls_delete():
    app = _make_app_with_gs()
    app.state.gs_sheet_name = "CV"

    app.sheets.delete("CV")

    assert app.state.gs_sheet_name is None
    assert "CV" in app.gs.sheet.deleted


def test_sheets_delete_missing_sheet_raises():
    app = _make_app_with_gs()

    with pytest.raises(ValueError, match="Sheet not found"):
        app.sheets.delete("missing")
