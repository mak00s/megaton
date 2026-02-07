import json
from types import SimpleNamespace

import gspread
import pandas as pd
import pytest
import requests

from megaton import errors
from megaton.gsheet import MegatonGS


def _new_gs():
    gs = MegatonGS.__new__(MegatonGS)
    return gs


def _api_error(message: str, code: int = 403) -> gspread.exceptions.APIError:
    resp = requests.Response()
    resp.status_code = code
    resp._content = json.dumps(
        {"error": {"code": code, "message": message, "status": message}}
    ).encode()
    return gspread.exceptions.APIError(resp)


class _DummyClient:
    def __init__(self, result):
        self.result = result

    def open_by_url(self, _url):
        if isinstance(self.result, Exception):
            raise self.result
        return self.result


class _FakeCell:
    def __init__(self, row, value):
        self.row = row
        self.value = value


class _FakeWorksheet:
    def __init__(self, title="Sheet1", row_count=5, col_count=3):
        self.title = title
        self.id = 123
        self.row_count = row_count
        self.col_count = col_count
        self.records = []
        self.cleared = False
        self.added_rows = 0
        self.added_cols = 0
        self.batch_clears = []
        self.frozen = None
        self.updated = {}
        self.last_acell = None

    def clear(self):
        self.cleared = True

    def get_all_records(self):
        return list(self.records)

    def range(self, _r1, _c1, _r2, _c2):
        return [_FakeCell(1, "h"), _FakeCell(2, "v")]

    def add_rows(self, n):
        self.row_count += n
        self.added_rows += n

    def add_cols(self, n):
        self.col_count += n
        self.added_cols += n

    def batch_clear(self, ranges):
        self.batch_clears.extend(ranges)

    def freeze(self, rows=None, cols=None):
        self.frozen = (rows, cols)

    def acell(self, address):
        self.last_acell = address
        return SimpleNamespace(value=f"value:{address}")

    def update(self, address, value):
        self.updated[address] = value


class _FakeSpreadsheet:
    def __init__(self, worksheet, worksheet_error=None):
        self._worksheet = worksheet
        self._worksheet_error = worksheet_error
        self.batch_updates = []
        self.deleted = None

    def worksheet(self, _name):
        if self._worksheet_error is not None:
            raise self._worksheet_error
        return self._worksheet

    def add_worksheet(self, title, rows, cols):
        self._worksheet = _FakeWorksheet(title=title, row_count=rows, col_count=cols)

    def del_worksheet(self, ws):
        self.deleted = ws

    def batch_update(self, payload):
        self.batch_updates.append(payload)


def _build_sheet(worksheet=None, worksheet_error=None):
    ws = worksheet or _FakeWorksheet()
    parent = SimpleNamespace(
        _client=True,
        _driver=_FakeSpreadsheet(ws, worksheet_error=worksheet_error),
    )
    sheet = MegatonGS.Sheet(parent)
    sheet._driver = ws
    return sheet, ws


def test_resolve_timeout_invalid_env_falls_back(monkeypatch):
    monkeypatch.setenv("MEGATON_GS_TIMEOUT", "invalid")
    gs = _new_gs()
    assert gs._resolve_timeout(None) == 180.0


def test_open_raises_bad_url_format():
    gs = _new_gs()
    gs._client = _DummyClient(gspread.exceptions.NoValidUrlKeyFound())

    with pytest.raises(errors.BadUrlFormat):
        gs.open("bad-url")


def test_open_raises_bad_permission_from_permission_error():
    gs = _new_gs()
    gs._client = _DummyClient(PermissionError("no permission"))

    with pytest.raises(errors.BadPermission):
        gs.open("https://docs.google.com/spreadsheets/d/xxxx")


def test_open_maps_api_errors():
    for message, expected in [
        ("disabled", errors.ApiDisabled),
        ("PERMISSION_DENIED", errors.BadPermission),
        ("NOT_FOUND", errors.UrlNotFound),
    ]:
        gs = _new_gs()

        class _Driver:
            @property
            def title(self):
                raise _api_error(message)

        gs._client = _DummyClient(_Driver())
        with pytest.raises(expected):
            gs.open("https://docs.google.com/spreadsheets/d/xxxx")


def test_open_success_with_sheet_selection():
    gs = _new_gs()
    selected = []
    driver = SimpleNamespace(title="Book", url="u")
    gs._client = _DummyClient(driver)
    gs.sheet = SimpleNamespace(select=lambda name: selected.append(name))

    title = gs.open("https://docs.google.com/spreadsheets/d/xxxx", sheet="Report")

    assert title == "Book"
    assert selected == ["Report"]


def test_sheet_select_raises_sheet_not_found():
    sheet, _ = _build_sheet(worksheet_error=gspread.exceptions.WorksheetNotFound("missing"))

    with pytest.raises(errors.SheetNotFound):
        sheet.select("missing")


def test_sheet_select_maps_api_error():
    for message, expected in [
        ("disabled", errors.ApiDisabled),
        ("PERMISSION_DENIED", errors.BadPermission),
    ]:
        sheet, _ = _build_sheet(worksheet_error=_api_error(message))
        with pytest.raises(expected):
            sheet.select("x")


def test_save_data_mode_w_maps_clear_errors(monkeypatch):
    sheet, ws = _build_sheet()
    df = pd.DataFrame([{"a": 1}])

    def _raise():
        raise _api_error("PERMISSION_DENIED")

    monkeypatch.setattr(sheet, "clear", _raise)
    with pytest.raises(errors.BadPermission):
        sheet.save_data(df, mode="w")

    def _raise2():
        raise _api_error("disabled")

    monkeypatch.setattr(sheet, "clear", _raise2)
    with pytest.raises(errors.ApiDisabled):
        sheet.save_data(df, mode="w")

    assert ws.cleared is False


def test_save_data_mode_a_adds_rows_and_writes(monkeypatch):
    sheet, ws = _build_sheet(worksheet=_FakeWorksheet(row_count=2, col_count=2))
    df = pd.DataFrame([{"a": 1}, {"a": 2}])
    calls = {}

    monkeypatch.setattr(sheet, "_refresh", lambda: None)

    def _record(*args, **kwargs):
        calls["args"] = args
        calls["kwargs"] = kwargs

    monkeypatch.setitem(sheet.save_data.__globals__, "set_with_dataframe", _record)
    result = sheet.save_data(df, mode="a", include_index=False)

    assert result is True
    assert ws.added_rows > 0
    assert calls["kwargs"]["include_column_header"] is False
    assert calls["kwargs"]["resize"] is False


def test_overwrite_data_from_row_delegates_when_row_le_1(monkeypatch):
    sheet, _ = _build_sheet()
    df = pd.DataFrame([{"a": 1}])

    monkeypatch.setattr(sheet, "overwrite_data", lambda _df, include_index=False: "ok")
    assert sheet.overwrite_data_from_row(df, row=1) == "ok"


def test_overwrite_data_from_row_maps_batch_clear_errors(monkeypatch):
    sheet, ws = _build_sheet()
    df = pd.DataFrame([{"a": 1}])

    def _raise(_ranges):
        raise _api_error("disabled")

    monkeypatch.setattr(ws, "batch_clear", _raise)
    with pytest.raises(errors.ApiDisabled):
        sheet.overwrite_data_from_row(df, row=2)


def test_overwrite_data_from_row_resizes_and_writes(monkeypatch):
    sheet, ws = _build_sheet(worksheet=_FakeWorksheet(row_count=2, col_count=1))
    df = pd.DataFrame([{"a": 1, "b": 2}, {"a": 3, "b": 4}])
    calls = {}

    monkeypatch.setattr(sheet, "_refresh", lambda: None)

    def _record(*args, **kwargs):
        calls["kwargs"] = kwargs

    monkeypatch.setitem(
        sheet.overwrite_data_from_row.__globals__,
        "set_with_dataframe",
        _record,
    )
    result = sheet.overwrite_data_from_row(df, row=3)

    assert result is True
    assert "3:2" in ws.batch_clears
    assert ws.added_rows > 0
    assert ws.added_cols > 0
    assert calls["kwargs"]["row"] == 3
    assert calls["kwargs"]["include_column_header"] is True


def test_cell_select_get_and_set_data():
    sheet, ws = _build_sheet()
    cell = MegatonGS.Sheet.Cell(sheet)

    value = cell.select(2, 3)
    assert value == "value:C2"

    cell.select("B2")
    cell.data = "x"
    assert ws.updated["B2"] == "x"
