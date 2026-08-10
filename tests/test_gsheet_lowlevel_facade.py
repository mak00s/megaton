from types import SimpleNamespace

import pandas as pd

from megaton import gsheet_lowlevel as gspread_lowlevel


def test_overwrite_worksheet_uses_one_atomic_batch_and_blanks_missing(monkeypatch):
    worksheet = SimpleNamespace(id=42, row_count=100, col_count=20)
    spreadsheet = object()
    captured = []
    monkeypatch.setattr(gspread_lowlevel, "get_or_create_worksheet", lambda *_a, **_kw: worksheet)
    monkeypatch.setattr(
        gspread_lowlevel,
        "batch_update_spreadsheet",
        lambda _spreadsheet, requests: captured.append(requests),
    )

    rows = gspread_lowlevel.overwrite_worksheet(
        spreadsheet,
        "data",
        pd.DataFrame({"value": [pd.NA], "when": [pd.NaT]}),
    )

    assert rows == 1
    assert len(captured) == 1
    requests = captured[0]
    assert requests[0]["updateCells"]["fields"] == "userEnteredValue"
    assert requests[1]["updateSheetProperties"]["properties"]["gridProperties"] == {
        "rowCount": 100,
        "columnCount": 20,
    }
    written = requests[2]["updateCells"]["rows"]
    assert written[1]["values"][0]["userEnteredValue"] == {"stringValue": ""}
    assert written[1]["values"][1]["userEnteredValue"] == {"stringValue": ""}


def test_overwrite_worksheet_preserves_raw_formula_semantics(monkeypatch):
    worksheet = SimpleNamespace(id=42, row_count=10, col_count=5)
    captured = []
    monkeypatch.setattr(gspread_lowlevel, "get_or_create_worksheet", lambda *_a, **_kw: worksheet)
    monkeypatch.setattr(
        gspread_lowlevel,
        "batch_update_spreadsheet",
        lambda _spreadsheet, requests: captured.append(requests),
    )

    gspread_lowlevel.overwrite_worksheet(
        object(),
        "data",
        pd.DataFrame({"value": ["=1+1"]}),
        value_input_option="RAW",
    )

    entered = captured[0][2]["updateCells"]["rows"][1]["values"][0]["userEnteredValue"]
    assert entered == {"stringValue": "=1+1"}


def test_overwrite_worksheet_does_not_write_unformatted_date_serial(monkeypatch):
    worksheet = SimpleNamespace(id=42, row_count=10, col_count=5)
    captured = []
    monkeypatch.setattr(gspread_lowlevel, "get_or_create_worksheet", lambda *_a, **_kw: worksheet)
    monkeypatch.setattr(
        gspread_lowlevel,
        "batch_update_spreadsheet",
        lambda _spreadsheet, requests: captured.append(requests),
    )

    gspread_lowlevel.overwrite_worksheet(
        object(),
        "data",
        pd.DataFrame({"date": ["2026-07-10"]}),
    )

    entered = captured[0][2]["updateCells"]["rows"][1]["values"][0]["userEnteredValue"]
    assert entered == {"stringValue": "2026-07-10"}


def test_overwrite_worksheet_validates_before_creating_sheet(monkeypatch):
    created = []
    monkeypatch.setattr(
        gspread_lowlevel,
        "get_or_create_worksheet",
        lambda *_a, **_kw: created.append(True),
    )

    try:
        gspread_lowlevel.overwrite_worksheet(
            object(),
            "data",
            pd.DataFrame({"value": [1]}),
            value_input_option="INVALID",
        )
    except ValueError as exc:
        assert "RAW or USER_ENTERED" in str(exc)

    assert created == []


def test_overwrite_worksheet_false_does_not_change_existing_freeze(monkeypatch):
    worksheet = SimpleNamespace(id=42, row_count=10, col_count=5)
    captured = []
    monkeypatch.setattr(gspread_lowlevel, "get_or_create_worksheet", lambda *_a, **_kw: worksheet)
    monkeypatch.setattr(
        gspread_lowlevel,
        "batch_update_spreadsheet",
        lambda _spreadsheet, requests: captured.append(requests),
    )

    gspread_lowlevel.overwrite_worksheet(
        object(),
        "data",
        pd.DataFrame({"value": [1]}),
        freeze_header=False,
    )

    grid_properties = [
        request["updateSheetProperties"]["properties"]["gridProperties"]
        for request in captured[0]
        if "updateSheetProperties" in request
    ]
    assert all("frozenRowCount" not in properties for properties in grid_properties)


def test_retrying_spreadsheet_wraps_worksheet_and_network_methods(monkeypatch):
    calls = []

    class Worksheet:
        title = "log"

        def append_row(self, row):
            return row

    class Spreadsheet:
        def worksheet(self, name):
            assert name == "log"
            return Worksheet()

        def batch_update(self, body):
            return body

    def retry(op, func, **_kwargs):
        calls.append(op)
        return func()

    monkeypatch.setattr(gspread_lowlevel, "call_with_retry", retry)
    spreadsheet = gspread_lowlevel.wrap_spreadsheet_with_retry(Spreadsheet())

    assert spreadsheet.worksheet("log").append_row(["x"]) == ["x"]
    assert spreadsheet.batch_update({"requests": []}) == {"requests": []}
    assert calls == ["worksheet log"]


def test_retrying_facade_does_not_retry_non_idempotent_mutations(monkeypatch):
    calls = []

    class Worksheet:
        title = "log"

        def append_rows(self, rows):
            calls.append(("append_rows", rows))
            return "appended"

        def delete_rows(self, start):
            calls.append(("delete_rows", start))
            return "deleted"

    class Spreadsheet:
        def worksheet(self, _name):
            return Worksheet()

        def add_worksheet(self, **kwargs):
            calls.append(("add_worksheet", kwargs))
            return Worksheet()

    retry_calls = []
    monkeypatch.setattr(
        gspread_lowlevel,
        "call_with_retry",
        lambda op, func, **_kwargs: retry_calls.append(op) or func(),
    )
    spreadsheet = gspread_lowlevel.wrap_spreadsheet_with_retry(Spreadsheet())
    worksheet = spreadsheet.worksheet("log")

    assert worksheet.append_rows([["x"]]) == "appended"
    assert worksheet.delete_rows(2) == "deleted"
    spreadsheet.add_worksheet(title="new", rows=10, cols=2)

    assert retry_calls == ["worksheet log"]
    assert calls == [
        ("append_rows", [["x"]]),
        ("delete_rows", 2),
        ("add_worksheet", {"title": "new", "rows": 10, "cols": 2}),
    ]


def test_append_rows_helper_submits_append_once_without_retry(monkeypatch):
    calls = []

    class Worksheet:
        def append_rows(self, rows, *, value_input_option):
            calls.append((rows, value_input_option))

    monkeypatch.setattr(
        gspread_lowlevel,
        "get_or_create_worksheet",
        lambda *_a, **_kw: Worksheet(),
    )
    retry_calls = []
    monkeypatch.setattr(
        gspread_lowlevel,
        "call_with_retry",
        lambda op, func, **_kwargs: retry_calls.append(op) or func(),
    )

    written = gspread_lowlevel.append_rows(object(), "log", [["x"]])

    assert written == 1
    assert calls == [([["x"]], "USER_ENTERED")]
    assert retry_calls == []


def test_wrap_spreadsheet_with_retry_is_idempotent():
    wrapped = gspread_lowlevel.RetryingSpreadsheet(object())
    assert gspread_lowlevel.wrap_spreadsheet_with_retry(wrapped) is wrapped


# --- batchUpdate retry classification (v2.1.1) ---

def _make_api_error(status):
    """Real gspread APIError so call_with_retry's exception tuple catches it."""
    import gspread

    class _Resp:
        status_code = status
        text = "err"

        @staticmethod
        def json():
            return {"error": {"code": status, "message": "err", "status": "ERR"}}

    return gspread.exceptions.APIError(_Resp())

def test_batch_update_retries_idempotent_batch(monkeypatch):
    calls = {"n": 0}

    class _SS:
        def batch_update(self, body):
            calls["n"] += 1
            if calls["n"] == 1:
                raise _make_api_error(503)
            return {"replies": []}

    monkeypatch.setattr("megaton.gsheet_lowlevel.time.sleep", lambda *_: None)
    out = gspread_lowlevel.batch_update_spreadsheet(
        _SS(), [{"updateCells": {}}, {"repeatCell": {}}]
    )
    assert out == {"replies": []}
    assert calls["n"] == 2  # retried


def test_batch_update_structural_batch_is_single_shot():
    calls = {"n": 0}

    class _SS:
        def batch_update(self, body):
            calls["n"] += 1
            raise _make_api_error(503)

    import pytest as _pytest

    with _pytest.raises(Exception):
        gspread_lowlevel.batch_update_spreadsheet(
            _SS(), [{"updateCells": {}}, {"appendDimension": {}}]
        )
    assert calls["n"] == 1  # not retried: batch contains a structural mutation


def test_batch_update_retry_flag_overrides():
    calls = {"n": 0}

    class _SS:
        def batch_update(self, body):
            calls["n"] += 1
            raise _make_api_error(503)

    import pytest as _pytest

    # Force off even for an idempotent batch
    with _pytest.raises(Exception):
        gspread_lowlevel.batch_update_spreadsheet(
            _SS(), [{"updateCells": {}}], retry=False
        )
    assert calls["n"] == 1


def test_structural_helpers_do_not_retry(monkeypatch):
    """ensure_sheet_exists submits addSheet once even on retryable errors."""
    calls = {"n": 0}

    class _SS:
        def fetch_sheet_metadata(self, params=None):
            return {"sheets": []}

        def batch_update(self, body):
            calls["n"] += 1
            assert "addSheet" in body["requests"][0]
            raise _make_api_error(503)

    import pytest as _pytest

    with _pytest.raises(Exception):
        gspread_lowlevel.ensure_sheet_exists(_SS(), "New")
    assert calls["n"] == 1
