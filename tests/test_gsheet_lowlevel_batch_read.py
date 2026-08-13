from __future__ import annotations

import pytest
from gspread.exceptions import WorksheetNotFound

from megaton import gsheet_lowlevel


class Spreadsheet:
    def __init__(self):
        self.metadata_calls = 0
        self.batch_calls: list[tuple[list[str], dict]] = []

    def fetch_sheet_metadata(self, *, params):
        self.metadata_calls += 1
        assert params == {"fields": "sheets.properties"}
        return {
            "sheets": [
                {"properties": {"title": "log", "sheetId": 1}},
                {"properties": {"title": "Bob's data", "sheetId": 2}},
                {"properties": {"title": "empty", "sheetId": 3}},
            ]
        }

    def values_batch_get(self, ranges, *, params):
        self.batch_calls.append((list(ranges), dict(params)))
        values = {
            "'log'": [["timestamp", "value"], ["2026-08-13", "1"]],
            "'Bob''s data'": [["name"], ["Bob"]],
            "'empty'": [],
        }
        return {
            "valueRanges": [
                {"range": range_name, "values": values[range_name]}
                for range_name in ranges
            ]
        }


def test_fetch_worksheets_values_batches_and_preserves_order():
    spreadsheet = Spreadsheet()

    result = gsheet_lowlevel.fetch_worksheets_values(
        spreadsheet,
        ["log", "missing", "Bob's data", "log", "empty"],
        missing_ok=True,
    )

    assert list(result) == ["log", "missing", "Bob's data", "empty"]
    assert result["log"][1] == ["2026-08-13", "1"]
    assert result["missing"] == []
    assert result["Bob's data"] == [["name"], ["Bob"]]
    assert result["empty"] == []
    assert spreadsheet.metadata_calls == 1
    assert spreadsheet.batch_calls[0][0] == ["'log'", "'Bob''s data'", "'empty'"]


def test_fetch_worksheets_values_uses_known_titles_without_metadata():
    spreadsheet = Spreadsheet()

    result = gsheet_lowlevel.fetch_worksheets_values(
        spreadsheet,
        ["log"],
        available_sheet_names=["log"],
    )

    assert result["log"][0] == ["timestamp", "value"]
    assert spreadsheet.metadata_calls == 0
    assert len(spreadsheet.batch_calls) == 1


def test_fetch_worksheets_values_missing_required_fails_before_values_read():
    spreadsheet = Spreadsheet()

    with pytest.raises(WorksheetNotFound):
        gsheet_lowlevel.fetch_worksheets_values(spreadsheet, ["log", "missing"])

    assert spreadsheet.metadata_calls == 1
    assert spreadsheet.batch_calls == []


def test_fetch_worksheets_values_empty_input_makes_no_requests():
    spreadsheet = Spreadsheet()

    assert gsheet_lowlevel.fetch_worksheets_values(spreadsheet, []) == {}
    assert spreadsheet.metadata_calls == 0
    assert spreadsheet.batch_calls == []
