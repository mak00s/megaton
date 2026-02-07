from types import SimpleNamespace

import pandas as pd
import pytest

from megaton import errors
from megaton.services.sheets_service import SheetsService


class _FakeSheet:
    def __init__(self, parent):
        self.parent = parent
        self._name = None
        self._driver = self
        self.updated_cells = {}

    @property
    def name(self):
        return self._name

    @property
    def data(self):
        if self._name is None:
            return []
        return self.parent._sheets.get(self._name, [])

    def select(self, name: str):
        if name not in self.parent._sheets:
            raise errors.SheetNotFound
        self._name = name
        return self._name

    def create(self, name: str):
        self.parent._sheets[name] = []
        self._name = name

    def overwrite_data(self, df, include_index=False):
        self.parent.last_write = ("overwrite_data", 1, include_index)
        self.parent.last_written = df.copy()
        self.parent._sheets[self._name] = df.to_dict(orient="records")
        return True

    def overwrite_data_from_row(self, df, row, include_index=False):
        self.parent.last_write = ("overwrite_data_from_row", row, include_index)
        existing = list(self.parent._sheets.get(self._name, []))
        kept = existing[: max(0, row - 1)]
        self.parent.last_written = df.copy()
        self.parent._sheets[self._name] = kept + df.to_dict(orient="records")
        return True

    def save_data(self, df, include_index=False):
        rows = self.parent._sheets.setdefault(self._name, [])
        rows.extend(df.to_dict(orient="records"))
        return True

    def freeze(self, rows=None, cols=None):
        self.parent.frozen = (rows, cols)

    def update_acell(self, cell, value):
        self.updated_cells[cell] = value


class _FakeGS:
    def __init__(self, url, sheets_data):
        self._sheets = dict(sheets_data)
        self._url = url
        self._title = "Fake Sheet"
        self.last_written = None
        self.last_write = None
        self.frozen = None
        self.sheet = _FakeSheet(self)
        self._driver = SimpleNamespace(batch_update=lambda payload: None)

    @property
    def url(self):
        return self._url

    @property
    def title(self):
        return self._title

    @property
    def sheets(self):
        return list(self._sheets.keys())


def _make_app(gs):
    return SimpleNamespace(
        gs=gs,
        creds=True,
        in_colab=False,
        state=SimpleNamespace(gs_url=None, gs_title=None, gs_sheet_name=None),
    )


def test_upsert_df_deduplicates_and_sorts():
    existing = [
        {"month": "2024-01", "clinic": "A", "users": 1},
        {"month": "2024-02 ", "clinic": "B", "users": 2},
    ]
    gs = _FakeGS("https://example.com/sheet", {"report": existing})
    app = _make_app(gs)
    gs.sheet.select("report")
    service = SheetsService(app)

    df_new = pd.DataFrame(
        [
            {"month": "2024-02", "clinic": "B", "users": 3},
            {"month": "2024-03", "clinic": "C", "users": 4},
        ]
    )

    result = service.upsert_df(
        "https://example.com/sheet",
        "report",
        df_new,
        keys=["month", "clinic"],
        columns=["month", "clinic", "users"],
        sort_by=["month", "clinic"],
    )

    assert result is not None
    assert app.state.gs_sheet_name == "report"
    assert list(gs.last_written.columns) == ["month", "clinic", "users"]
    assert gs.last_written.to_dict(orient="records") == [
        {"month": "2024-01", "clinic": "A", "users": 1},
        {"month": "2024-02", "clinic": "B", "users": 3},
        {"month": "2024-03", "clinic": "C", "users": 4},
    ]


def test_append_sheet_applies_write_options():
    gs = _FakeGS("https://example.com/sheet", {"report": [{"a": 1}]})
    app = _make_app(gs)
    gs.sheet.select("report")
    service = SheetsService(app)

    calls = {}

    def _record(df_for_width, **kwargs):
        calls["df"] = df_for_width
        calls["kwargs"] = kwargs

    service._apply_write_options = _record
    service.append_sheet(
        "report",
        pd.DataFrame([{"a": 2}]),
        auto_width=True,
        freeze_header=True,
    )

    assert calls["kwargs"]["auto_width"] is True
    assert calls["kwargs"]["freeze_header"] is True
    assert calls["df"].to_dict(orient="records") == [{"a": 1}, {"a": 2}]


def test_save_sheet_start_row_uses_partial_overwrite_and_preserves_upper_rows():
    gs = _FakeGS(
        "https://example.com/sheet",
        {"report": [{"meta": "keep"}, {"month": "2024-01", "users": 1}]},
    )
    app = _make_app(gs)
    gs.sheet.select("report")
    service = SheetsService(app)

    calls = {}

    def _record(df_for_width, **kwargs):
        calls["df"] = df_for_width
        calls["kwargs"] = kwargs

    service._apply_write_options = _record
    service.save_sheet(
        "report",
        pd.DataFrame([{"month": "2024-02", "users": 2}]),
        start_row=2,
        auto_width=True,
        freeze_header=True,
    )

    assert gs.last_write == ("overwrite_data_from_row", 2, False)
    assert gs._sheets["report"] == [{"meta": "keep"}, {"month": "2024-02", "users": 2}]
    assert calls["kwargs"]["auto_width"] is True
    assert calls["kwargs"]["freeze_header"] is True
    assert calls["df"].to_dict(orient="records") == [{"month": "2024-02", "users": 2}]


def test_save_sheet_default_start_row_uses_full_overwrite():
    gs = _FakeGS("https://example.com/sheet", {"report": [{"meta": "old"}]})
    app = _make_app(gs)
    gs.sheet.select("report")
    service = SheetsService(app)

    service.save_sheet(
        "report",
        pd.DataFrame([{"month": "2024-02", "users": 2}]),
    )

    assert gs.last_write == ("overwrite_data", 1, False)
    assert gs._sheets["report"] == [{"month": "2024-02", "users": 2}]


def test_save_sheet_rejects_invalid_start_row():
    gs = _FakeGS("https://example.com/sheet", {"report": []})
    app = _make_app(gs)
    service = SheetsService(app)

    with pytest.raises(ValueError, match="start_row"):
        service.save_sheet("report", pd.DataFrame([{"a": 1}]), start_row=0)


def test_upsert_df_applies_write_options():
    gs = _FakeGS(
        "https://example.com/sheet",
        {"report": [{"month": "2024-01", "clinic": "A", "users": 1}]},
    )
    app = _make_app(gs)
    gs.sheet.select("report")
    service = SheetsService(app)

    calls = {}

    def _record(df_for_width, **kwargs):
        calls["df"] = df_for_width
        calls["kwargs"] = kwargs

    service._apply_write_options = _record

    result = service.upsert_df(
        "https://example.com/sheet",
        "report",
        pd.DataFrame([{"month": "2024-02", "clinic": "B", "users": 2}]),
        keys=["month", "clinic"],
        auto_width=True,
        freeze_header=True,
    )

    assert result is not None
    assert calls["kwargs"]["auto_width"] is True
    assert calls["kwargs"]["freeze_header"] is True
    assert calls["df"].to_dict(orient="records") == result.to_dict(orient="records")
