from types import SimpleNamespace

import pandas as pd
import pytest

from megaton import errors
from megaton import gsheet_lowlevel
from megaton.gsheet import MegatonGS
from megaton.services.sheets_service import SheetsService


class _FakeSheet:
    def __init__(self, parent):
        self.parent = parent
        self._name = None
        self._driver = self
        self.updated_cells = {}
        self.updated_ranges = {}
        self.duplicated = []
        self.fail_update_acell = False

    @property
    def name(self):
        return self._name

    @property
    def data(self):
        if self._name is None:
            return []
        return self.parent._sheets.get(self._name, [])

    def get_records(self, **_kwargs):
        self.parent.last_read_options = _kwargs
        return self.data

    def select(self, name: str):
        if name not in self.parent._sheets:
            raise errors.SheetNotFound
        self._name = name
        return self._name

    def create(self, name: str):
        self.parent._sheets[name] = []
        self._name = name

    def duplicate(self, source_name: str, new_sheet_name: str, **_kwargs):
        if source_name not in self.parent._sheets:
            raise errors.SheetNotFound
        self.parent._sheets[new_sheet_name] = list(self.parent._sheets[source_name])
        self.duplicated.append((source_name, new_sheet_name))
        self._name = new_sheet_name
        return new_sheet_name

    def overwrite_data(self, df, include_index=False, **_kwargs):
        self.parent.last_write = ("overwrite_data", 1, include_index)
        self.parent.last_written = df.copy()
        self.parent._sheets[self._name] = df.to_dict(orient="records")
        return True

    def overwrite_data_from_row(self, df, row, include_index=False, **_kwargs):
        self.parent.last_write = ("overwrite_data_from_row", row, include_index)
        existing = list(self.parent._sheets.get(self._name, []))
        kept = existing[: max(0, row - 1)]
        self.parent.last_written = df.copy()
        self.parent._sheets[self._name] = kept + df.to_dict(orient="records")
        return True

    def save_data(self, df, include_index=False, **_kwargs):
        rows = self.parent._sheets.setdefault(self._name, [])
        rows.extend(df.to_dict(orient="records"))
        return True

    def freeze(self, rows=None, cols=None, **_kwargs):
        self.parent.frozen = (rows, cols)

    def update_acell(self, cell, value):
        if self.fail_update_acell:
            raise RuntimeError("cell update failed")
        self.updated_cells[cell] = value

    def update(self, a1_range, values):
        self.updated_ranges[a1_range] = values


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


class _RoundTripSheet(_FakeSheet):
    """Execute real record reading and atomic cell building against a fake grid."""

    def __init__(self, parent):
        super().__init__(parent)
        self.cells = []
        self.ws = SimpleNamespace(
            title="report", id=1, row_count=100, col_count=10,
            get_all_records=self._records,
        )
        api = SimpleNamespace(batch_update=self._batch, fetch_sheet_metadata=self._metadata)
        self.real = MegatonGS.Sheet(SimpleNamespace(_driver=api))
        self.real._driver = self.ws

    def _records(self, **kwargs):
        headers = [cell["userEnteredValue"]["stringValue"] for cell in self.cells[0]]
        return [dict(zip(headers, [next(iter(cell.get("userEnteredValue", {}).values()), "")
                                  for cell in row])) for row in self.cells[1:]] if self.cells else []

    def _metadata(self, **kwargs):
        self.last_metadata_params = kwargs["params"]
        return {"sheets": [{"data": [{"rowData": [{"values": [
            {"effectiveValue": cell.get("userEnteredValue", {}),
             "effectiveFormat": cell.get("userEnteredFormat", {})} for cell in row
        ]} for row in self.cells]}]}]}

    def _batch(self, payload):
        for request in payload["requests"]:
            update = request.get("updateCells", {})
            if "rows" in update:
                old = self.cells
                self.cells = [record["values"] for record in update["rows"]]
                for ri, row in enumerate(self.cells):
                    for ci, cell in enumerate(row):
                        cell.pop("userEnteredFormat", None)
                        if ri < len(old) and ci < len(old[ri]):
                            cell["userEnteredFormat"] = old[ri][ci].get("userEnteredFormat", {})
            repeat = request.get("repeatCell", {})
            if repeat:
                region = repeat["range"]
                for ri in range(region["startRowIndex"], region["endRowIndex"]):
                    self.cells[ri][region["startColumnIndex"]]["userEnteredFormat"] = repeat["cell"]["userEnteredFormat"]

    def get_records(self, **kwargs):
        if not self.cells:
            return []
        return self.real.get_records(**kwargs)

    def overwrite_data(self, df, include_index=False, **kwargs):
        self.parent.last_written = df.copy()
        return self.real._atomic_overwrite(
            df, row=1, include_index=include_index, max_retries=None, backoff_factor=None,
        )


@pytest.mark.parametrize("date", [
    "2024-01-01", "2024-01-01 10:30:15", pd.Timestamp("2024-01-01 10:30:15"),
    pd.Timestamp("2024-01-01 10:30:15.123456"),
])
def test_upsert_date_keys_survive_real_cell_roundtrip(date):
    gs = _FakeGS("https://example.com/sheet", {"report": []})
    gs.sheet = _RoundTripSheet(gs)
    service = SheetsService(_make_app(gs))
    for count in (1, 2, 3):
        result = service.upsert_df(gs.url, "report", pd.DataFrame([{"date": date, "count": count}]), keys=["date"])
        assert len(result) == 1
        assert result["count"].tolist() == [count]
        assert len(gs.sheet.cells) == 2


def test_upsert_preserves_distinct_subsecond_keys_and_nonkey_date_formats():
    gs = _FakeGS("https://example.com/sheet", {"report": []})
    gs.sheet = _RoundTripSheet(gs)
    service = SheetsService(_make_app(gs))
    first = pd.DataFrame([
        {"key": "2024-01-01 10:30:15.100000", "n": 9, "date": "2024-01-02"},
        {"key": "2024-01-01 10:30:15.200000", "n": 100, "date": "2024-01-03"},
    ])
    service.upsert_df(gs.url, "report", first, keys=["key"], sort_by=["n"])
    second = pd.DataFrame([{"key": "2024-01-01 10:30:15.100000", "n": 101, "date": "2024-01-02"}])
    result = service.upsert_df(gs.url, "report", second, keys=["key"], sort_by=["n"])
    assert len(result) == 2
    assert result["n"].tolist() == [100, 101]
    for row, expected in zip(gs.sheet.cells[1:], ("2024-01-03", "2024-01-02")):
        assert row[2]["userEnteredValue"] == gsheet_lowlevel.cell_data(expected)["userEnteredValue"]
        assert row[2]["userEnteredFormat"]["numberFormat"]["type"] == "DATE"


def test_retained_nonkey_date_moves_to_unformatted_row_with_original_format():
    gs = _FakeGS("https://example.com/sheet", {"report": []})
    gs.sheet = _RoundTripSheet(gs)
    service = SheetsService(_make_app(gs))
    service.upsert_df(gs.url, "report", pd.DataFrame([
        {"key": "a", "n": 100, "date": "2024-01-02"},
        {"key": "b", "n": 9, "date": ""},
    ]), keys=["key"])
    original = {"type": "DATE", "pattern": "dd/mm/yyyy"}
    gs.sheet.cells[1][2]["userEnteredFormat"]["numberFormat"] = original
    service.upsert_df(gs.url, "report", pd.DataFrame([
        {"key": "c", "n": 10, "date": ""},
    ]), keys=["key"], sort_by=["n"])
    assert gs.sheet.cells[3][0]["userEnteredValue"]["stringValue"] == "a"
    assert gs.sheet.cells[3][2]["userEnteredValue"] == {"numberValue": 45293.0}
    assert gs.sheet.cells[3][2]["userEnteredFormat"]["numberFormat"] == original


def test_upsert_numeric_sort_and_no_dataframe_map(monkeypatch):
    monkeypatch.delattr(pd.DataFrame, "map", raising=False)
    gs = _FakeGS("https://example.com/sheet", {"report": [{"key": "a", "n": 9}, {"key": "b", "n": 100}]})
    result = SheetsService(_make_app(gs)).upsert_df(
        gs.url, "report", pd.DataFrame([{"key": "c", "n": 10}]), keys=["key"], sort_by=["n"],
    )
    assert result["n"].tolist() == [9, 10, 100]


@pytest.mark.parametrize("date", ["2024-01-01", "2024-01-01 10:30:15.123456"])
def test_upsert_returns_public_dates_and_sizes_without_internal_objects(date):
    gs = _FakeGS("https://example.com/sheet", {"report": []})
    gs.sheet = _RoundTripSheet(gs)
    gs.sheet.id = 1
    widths = []
    gs._driver.batch_update = widths.append
    service = SheetsService(_make_app(gs))
    service.upsert_df(gs.url, "report", pd.DataFrame([{"key": "a", "date": date}]), keys=["key"])
    original_cell = gs.sheet.cells[1][1]["userEnteredValue"].copy()
    result = service.upsert_df(gs.url, "report", pd.DataFrame([
        {"key": "b", "date": "2024-01-02"},
    ]), keys=["key"], auto_width=True)
    assert not any(isinstance(value, gsheet_lowlevel._NativeDate) for value in result["date"])
    assert pd.Timestamp(result["date"].iloc[0]) == pd.Timestamp(date)
    assert "_NativeDate" not in result.to_csv(index=False)
    assert (pd.to_datetime(result["date"].map(pd.Timestamp)) - pd.Timestamp("2024-01-01")).notna().all()
    date_width = widths[-1]["requests"][1]["updateDimensionProperties"]["properties"]["pixelSize"]
    assert 50 <= date_width < 300
    assert gs.sheet.cells[1][1]["userEnteredValue"] == original_cell
    assert gs.sheet.last_metadata_params["ranges"] == "'report'!A1:B2"
    empty = service.upsert_df(gs.url, "report", pd.DataFrame(columns=["key", "date"]), keys=["key"])
    assert not any(isinstance(value, gsheet_lowlevel._NativeDate) for value in empty["date"])
    assert len(empty) == 2


def test_date_serial_and_textual_serial_are_distinct_upsert_keys():
    gs = _FakeGS("https://example.com/sheet", {"report": []})
    gs.sheet = _RoundTripSheet(gs)
    service = SheetsService(_make_app(gs))
    incoming = pd.DataFrame([{"key": "2024-01-01", "n": 1}, {"key": "45292", "n": 2}])
    for _ in range(3):
        result = service.upsert_df(gs.url, "report", incoming, keys=["key"])
        assert len(result) == 2
        assert result["n"].sum() == 3


def test_upsert_numeric_keys_are_not_inferred_as_dates():
    gs = _FakeGS("https://example.com/sheet", {"report": []})
    gs.sheet = _RoundTripSheet(gs)
    service = SheetsService(_make_app(gs))
    for _ in range(2):
        result = service.upsert_df(gs.url, "report", pd.DataFrame([
            {"key": 45292, "n": 1}, {"key": "0012", "n": 2},
        ]), keys=["key"])
        assert len(result) == 2
    assert any(isinstance(value, (int, float)) and value == 45292 for value in result["key"])
    assert any(value == "0012" for value in result["key"])
    assert all(cell.get("userEnteredFormat", {}).get("numberFormat", {}).get("type") != "DATE"
               for row in gs.sheet.cells[1:] for cell in row)


def test_incomplete_date_keys_are_idempotent_after_cell_roundtrip():
    gs = _FakeGS("https://example.com/sheet", {"report": []})
    gs.sheet = _RoundTripSheet(gs)
    service = SheetsService(_make_app(gs))
    incoming = pd.DataFrame([{"date": "2024-01-01", "medium": None, "n": 1}])
    for _ in range(3):
        result = service.upsert_df(gs.url, "report", incoming, keys=["date", "medium"])
        assert len(result) == 1


def test_upsert_sorts_native_and_new_dates_on_the_same_timeline():
    gs = _FakeGS("https://example.com/sheet", {"report": []})
    gs.sheet = _RoundTripSheet(gs)
    service = SheetsService(_make_app(gs))
    service.upsert_df(gs.url, "report", pd.DataFrame([
        {"date": "1899-12-29", "n": 1}, {"date": "1900-01-09", "n": 3},
    ]), keys=["date"])
    result = service.upsert_df(gs.url, "report", pd.DataFrame([
        {"date": pd.Timestamp("1899-12-31"), "n": 2},
    ]), keys=["date"])
    assert result["n"].tolist() == [1, 2, 3]


def test_upsert_mixed_sort_fallback_preserves_numeric_secondary_sort():
    gs = _FakeGS("https://example.com/sheet", {"report": [{"key": "a", "group": 1, "n": 100}]})
    result = SheetsService(_make_app(gs)).upsert_df(gs.url, "report", pd.DataFrame([
        {"key": "b", "group": "1", "n": 9}, {"key": "c", "group": "1", "n": 10},
    ]), keys=["key"], sort_by=["group", "n"])
    assert result["n"].tolist() == [9, 10, 100]


def test_upsert_preserves_numeric_and_text_key_types():
    gs = _FakeGS("https://example.com/sheet", {"report": [
        {"month": 202608, "code": "0012", "users": 1},
        {"month": 202609, "code": "0012", "users": 2},
    ]})
    gs.sheet.select("report")
    service = SheetsService(_make_app(gs))
    result = service.upsert_df("https://example.com/sheet", "report", pd.DataFrame([
        {"month": 202609, "code": "0012", "users": 3},
    ]), keys=["month", "code"])
    assert result["month"].tolist() == [202608, 202609]
    assert result["code"].tolist() == ["0012", "0012"]
    assert result["users"].tolist() == [1, 3]


def test_upsert_date_keys_match_formatted_history_without_duplicates():
    gs = _FakeGS("https://example.com/sheet", {"report": [
        {"ym": "2026/8/1", "count": 1}, {"ym": "2026/9/1", "count": 2}]})
    gs.sheet.select("report")
    service = SheetsService(_make_app(gs))
    result = service.upsert_df("https://example.com/sheet", "report", pd.DataFrame([
        {"ym": pd.Timestamp("2026-09-01"), "count": 3}]), keys=["ym"])
    assert result["ym"].tolist() == [pd.Timestamp("2026-08-01"), pd.Timestamp("2026-09-01")]
    assert result["count"].tolist() == [1, 3]


@pytest.mark.parametrize("missing", [None, float("nan"), pd.NA, ""])
def test_upsert_blank_keys_remain_idempotent_and_preserve_history(missing):
    gs = _FakeGS("https://example.com/sheet", {"report": [
        {"month": 202608, "medium": "", "users": 0},
        {"month": 202609, "medium": "", "users": 0},
        {"month": 202609, "medium": "", "users": 0},
        {"month": 202609, "medium": "organic", "users": 2}]})
    gs.sheet.select("report")
    service = SheetsService(_make_app(gs))
    incoming = pd.DataFrame([
        {"month": 202609, "medium": missing, "users": 0},
        {"month": 202609, "medium": "organic", "users": 3}])
    for _ in range(2):
        service.upsert_df("https://example.com/sheet", "report", incoming, keys=["month", "medium"])
        assert len(gs.last_written) == 4
        assert gs.last_written["users"].sum() == 3
        assert len(gs.last_written[gs.last_written["medium"].eq("")]) == 3


def test_upsert_blank_key_ambiguous_change_stops_before_write():
    gs = _FakeGS("https://example.com/sheet", {"report": [
        {"medium": "", "users": 1}, {"medium": "", "users": 2}]})
    service = SheetsService(_make_app(gs))
    with pytest.raises(ValueError, match="ambiguous historical"):
        service.upsert_df("https://example.com/sheet", "report",
                          pd.DataFrame([{"medium": None, "users": 3}]), keys=["medium"])
    assert gs.last_written is None


def test_upsert_literal_nan_is_not_a_blank_key():
    gs = _FakeGS("https://example.com/sheet", {"report": [{"medium": "nan", "users": 2}]})
    service = SheetsService(_make_app(gs))
    result = service.upsert_df("https://example.com/sheet", "report",
                              pd.DataFrame([{"medium": None, "users": 0}]), keys=["medium"])
    assert len(result) == 2
    assert result["users"].sum() == 2
    assert gs.last_read_options["value_render_option"] == "UNFORMATTED_VALUE"
    assert gs.last_read_options["numericise_ignore"] == ["all"]


def test_upsert_native_date_serial_keys_match_datetime_input():
    serial = (pd.Timestamp("2026-09-01") - pd.Timestamp("1899-12-30")).days
    gs = _FakeGS("https://example.com/sheet", {"report": [{"ym": serial, "count": 1}]})
    service = SheetsService(_make_app(gs))
    result = service.upsert_df("https://example.com/sheet", "report",
                              pd.DataFrame([{"ym": pd.Timestamp("2026-09-01"), "count": 3}]), keys=["ym"])
    assert result["ym"].tolist() == [pd.Timestamp("2026-09-01")]
    assert result["count"].tolist() == [3]


def test_upsert_new_null_key_is_idempotent_after_sheets_blank_roundtrip():
    gs = _FakeGS("https://example.com/sheet", {"report": []})
    service = SheetsService(_make_app(gs))
    incoming = pd.DataFrame([{"medium": pd.NA, "source": None, "users": 0}])
    for _ in range(3):
        result = service.upsert_df("https://example.com/sheet", "report", incoming,
                                   keys=["medium", "source"])
        assert len(result) == 1
        gs._sheets["report"] = result.astype(object).where(pd.notna(result), "").to_dict("records")


def test_upsert_single_blank_history_row_can_be_updated():
    gs = _FakeGS("https://example.com/sheet", {"report": [{"medium": "", "users": 1}]})
    service = SheetsService(_make_app(gs))
    result = service.upsert_df("https://example.com/sheet", "report",
                              pd.DataFrame([{"medium": None, "users": 3}]), keys=["medium"])
    assert len(result) == 1
    assert result["users"].sum() == 3


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


def test_save_sheet_can_create_missing_sheet():
    gs = _FakeGS("https://example.com/sheet", {"existing": [{"a": 1}]})
    app = _make_app(gs)
    service = SheetsService(app)

    service.save_sheet(
        "new_report",
        pd.DataFrame([{"month": "2024-02", "users": 2}]),
        create_if_missing=True,
    )

    assert "new_report" in gs.sheets
    assert app.state.gs_sheet_name == "new_report"
    assert gs.last_write == ("overwrite_data", 1, False)


def test_append_sheet_can_create_missing_sheet():
    gs = _FakeGS("https://example.com/sheet", {"existing": [{"a": 1}]})
    app = _make_app(gs)
    service = SheetsService(app)

    service.append_sheet(
        "new_report",
        pd.DataFrame([{"month": "2024-02", "users": 2}]),
        create_if_missing=True,
    )

    assert "new_report" in gs.sheets
    assert app.state.gs_sheet_name == "new_report"
    assert gs._sheets["new_report"] == [{"month": "2024-02", "users": 2}]


def test_save_sheet_missing_sheet_without_create_does_nothing():
    gs = _FakeGS("https://example.com/sheet", {"existing": [{"a": 1}]})
    app = _make_app(gs)
    service = SheetsService(app)

    with pytest.raises(errors.SheetNotFound):
        service.save_sheet(
            "missing",
            pd.DataFrame([{"month": "2024-02", "users": 2}]),
            create_if_missing=False,
        )

    assert "missing" not in gs.sheets
    assert gs.last_write is None


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


def test_select_or_create_sheet_raises_without_open_sheet():
    app = _make_app(None)
    service = SheetsService(app)

    with pytest.raises(ValueError):
        service._select_or_create_sheet("report", create_if_missing=True)


def test_select_or_create_sheet_propagates_create_failure(monkeypatch):
    gs = _FakeGS("https://example.com/sheet", {"report": []})
    app = _make_app(gs)
    service = SheetsService(app)

    def _raise(_name):
        raise RuntimeError("create failed")

    monkeypatch.setattr(gs.sheet, "create", _raise)
    with pytest.raises(RuntimeError):
        service._select_or_create_sheet("new_sheet", create_if_missing=True)


def test_append_sheet_missing_sheet_without_create_does_nothing():
    gs = _FakeGS("https://example.com/sheet", {"report": [{"a": 1}]})
    app = _make_app(gs)
    service = SheetsService(app)

    with pytest.raises(errors.SheetNotFound):
        service.append_sheet(
            "missing",
            pd.DataFrame([{"a": 2}]),
            create_if_missing=False,
        )

    assert "missing" not in gs.sheets


def test_open_or_create_sheet_returns_none_when_open_sheet_fails(monkeypatch):
    gs = _FakeGS("https://example.com/a", {"report": []})
    app = _make_app(gs)
    service = SheetsService(app)
    monkeypatch.setattr(service, "open_sheet", lambda _url: False)

    assert service.open_or_create_sheet("https://example.com/b", "report") is None


def test_open_or_create_sheet_selects_existing_sheet():
    gs = _FakeGS("https://example.com/sheet", {"report": []})
    app = _make_app(gs)
    service = SheetsService(app)

    result = service.open_or_create_sheet("https://example.com/sheet", "report")

    assert result is True
    assert app.state.gs_sheet_name == "report"


def test_open_or_create_sheet_creates_when_missing():
    gs = _FakeGS("https://example.com/sheet", {"report": []})
    app = _make_app(gs)
    service = SheetsService(app)

    result = service.open_or_create_sheet("https://example.com/sheet", "new_sheet")

    assert result is True
    assert "new_sheet" in gs.sheets
    assert app.state.gs_sheet_name == "new_sheet"


def test_duplicate_sheet_success():
    gs = _FakeGS("https://example.com/sheet", {"template": [{"a": 1}]})
    app = _make_app(gs)
    service = SheetsService(app)
    monkeypatch = pytest.MonkeyPatch()
    calls = []
    monkeypatch.setattr(service, "open_sheet", lambda _url: calls.append(_url) or True)

    result = service.duplicate_sheet(
        "https://example.com/sheet",
        "template",
        "report_2024_02",
        cell_update={"cell": "B1", "value": "202402"},
    )
    monkeypatch.undo()

    assert result is True
    assert calls == ["https://example.com/sheet"]
    assert ("template", "report_2024_02") in gs.sheet.duplicated
    assert gs.sheet.updated_cells["B1"] == "202402"
    assert app.state.gs_sheet_name == "report_2024_02"


def test_duplicate_sheet_returns_true_when_cell_update_fails(capsys):
    gs = _FakeGS("https://example.com/sheet", {"template": [{"a": 1}]})
    gs.sheet.fail_update_acell = True
    app = _make_app(gs)
    service = SheetsService(app)
    monkeypatch = pytest.MonkeyPatch()
    monkeypatch.setattr(service, "open_sheet", lambda _url: True)

    result = service.duplicate_sheet(
        "https://example.com/sheet",
        "template",
        "report_2024_02",
        cell_update={"cell": "B1", "value": "202402"},
    )
    monkeypatch.undo()

    out = capsys.readouterr().out
    assert result is True
    assert ("template", "report_2024_02") in gs.sheet.duplicated
    assert app.state.gs_sheet_name == "report_2024_02"
    assert "セル B1 の更新には失敗しました" in out


def test_duplicate_sheet_returns_none_when_source_missing():
    gs = _FakeGS("https://example.com/sheet", {"report": []})
    app = _make_app(gs)
    service = SheetsService(app)
    monkeypatch = pytest.MonkeyPatch()
    monkeypatch.setattr(service, "open_sheet", lambda _url: True)

    result = service.duplicate_sheet(
        "https://example.com/sheet",
        "missing",
        "copy",
    )
    monkeypatch.undo()

    assert result is None


def test_duplicate_sheet_returns_none_when_open_sheet_fails(monkeypatch):
    gs = _FakeGS("https://example.com/sheet", {"template": []})
    app = _make_app(gs)
    service = SheetsService(app)
    monkeypatch.setattr(service, "open_sheet", lambda _url: False)

    assert service.duplicate_sheet("https://example.com/sheet", "template", "copy") is None


def test_open_or_create_sheet_returns_none_when_create_fails(monkeypatch):
    gs = _FakeGS("https://example.com/sheet", {"report": []})
    app = _make_app(gs)
    service = SheetsService(app)

    def _raise(_name):
        raise RuntimeError("create failed")

    monkeypatch.setattr(gs.sheet, "create", _raise)
    assert service.open_or_create_sheet("https://example.com/sheet", "new_sheet") is None


def test_read_df_raises_when_open_fails(monkeypatch):
    gs = _FakeGS("https://example.com/sheet", {"report": [{"a": 1}]})
    app = _make_app(gs)
    service = SheetsService(app)
    monkeypatch.setattr(service, "open_sheet", lambda _url: False)

    with pytest.raises(errors.RequestError):
        service.read_df("https://example.com/sheet", "report")



def test_read_df_raises_when_sheet_not_found():
    gs = _FakeGS("https://example.com/sheet", {"report": [{"a": 1}]})
    app = _make_app(gs)
    service = SheetsService(app)
    monkeypatch = pytest.MonkeyPatch()
    monkeypatch.setattr(service, "open_sheet", lambda _url: True)

    with pytest.raises(errors.SheetNotFound):
        service.read_df("https://example.com/sheet", "missing")
    monkeypatch.undo()



def test_read_df_raises_on_unexpected_select_error(monkeypatch):
    gs = _FakeGS("https://example.com/sheet", {"report": [{"a": 1}]})
    app = _make_app(gs)
    service = SheetsService(app)
    monkeypatch.setattr(service, "open_sheet", lambda _url: True)

    def _raise(_name):
        raise RuntimeError("broken select")

    monkeypatch.setattr(gs.sheet, "select", _raise)
    with pytest.raises(errors.RequestError):
        service.read_df("https://example.com/sheet", "report")



def test_read_df_success_returns_dataframe():
    gs = _FakeGS("https://example.com/sheet", {"report": [{"a": 1}, {"a": 2}]})
    app = _make_app(gs)
    service = SheetsService(app)
    monkeypatch = pytest.MonkeyPatch()
    monkeypatch.setattr(service, "open_sheet", lambda _url: True)

    result = service.read_df("https://example.com/sheet", "report")
    monkeypatch.undo()

    assert result.to_dict(orient="records") == [{"a": 1}, {"a": 2}]


def test_upsert_df_rejects_non_dataframe():
    gs = _FakeGS("https://example.com/sheet", {"report": []})
    app = _make_app(gs)
    service = SheetsService(app)

    with pytest.raises(TypeError, match="pandas.DataFrame"):
        service.upsert_df("https://example.com/sheet", "report", [{"a": 1}], keys=["a"])


def test_upsert_df_raises_when_open_or_create_fails(monkeypatch):
    gs = _FakeGS("https://example.com/sheet", {"report": []})
    app = _make_app(gs)
    service = SheetsService(app)
    monkeypatch.setattr(service, "open_or_create_sheet", lambda *_args, **_kwargs: None)

    with pytest.raises(errors.RequestError):
        service.upsert_df(
            "https://example.com/sheet",
            "report",
            pd.DataFrame([{"a": 1}]),
            keys=["a"],
            create_if_missing=True,
        )



def test_upsert_df_create_false_raises_when_open_sheet_fails(monkeypatch):
    gs = _FakeGS("https://example.com/sheet", {"report": []})
    app = _make_app(gs)
    service = SheetsService(app)
    monkeypatch.setattr(service, "open_sheet", lambda _url: False)

    with pytest.raises(errors.RequestError):
        service.upsert_df(
            "https://example.com/sheet",
            "report",
            pd.DataFrame([{"a": 1}]),
            keys=["a"],
            create_if_missing=False,
        )



def test_upsert_df_create_false_raises_when_sheet_missing():
    gs = _FakeGS("https://example.com/sheet", {"report": []})
    app = _make_app(gs)
    service = SheetsService(app)
    monkeypatch = pytest.MonkeyPatch()
    monkeypatch.setattr(service, "open_sheet", lambda _url: True)

    with pytest.raises(errors.SheetNotFound):
        service.upsert_df(
            "https://example.com/sheet",
            "missing",
            pd.DataFrame([{"a": 1}]),
            keys=["a"],
            create_if_missing=False,
        )
    monkeypatch.undo()



def test_upsert_df_empty_existing_raises_when_write_fails(monkeypatch):
    gs = _FakeGS("https://example.com/sheet", {"report": []})
    app = _make_app(gs)
    service = SheetsService(app)
    gs.sheet.select("report")

    def _raise(_df, include_index=False, **kwargs):
        raise RuntimeError("write failed")

    monkeypatch.setattr(gs.sheet, "overwrite_data", _raise)
    with pytest.raises(RuntimeError):
        service.upsert_df(
            "https://example.com/sheet",
            "report",
            pd.DataFrame([{"a": 1}]),
            keys=["a"],
        )



def test_upsert_df_raises_when_keys_missing():
    gs = _FakeGS("https://example.com/sheet", {"report": [{"a": 1}]})
    app = _make_app(gs)
    service = SheetsService(app)
    gs.sheet.select("report")

    with pytest.raises(ValueError):
        service.upsert_df(
            "https://example.com/sheet",
            "report",
            pd.DataFrame([{"a": 2}]),
            keys=["missing_key"],
        )



def test_upsert_df_raises_when_final_write_fails(monkeypatch):
    gs = _FakeGS("https://example.com/sheet", {"report": [{"a": 1}]})
    app = _make_app(gs)
    service = SheetsService(app)
    gs.sheet.select("report")

    def _raise(_df, include_index=False, **kwargs):
        raise RuntimeError("final write failed")

    monkeypatch.setattr(gs.sheet, "overwrite_data", _raise)
    with pytest.raises(RuntimeError):
        service.upsert_df(
            "https://example.com/sheet",
            "report",
            pd.DataFrame([{"a": 2}]),
            keys=["a"],
        )



def test_update_cells_returns_none_when_updates_empty():
    gs = _FakeGS("https://example.com/sheet", {"report": []})
    app = _make_app(gs)
    service = SheetsService(app)

    assert service.update_cells("https://example.com/sheet", "report", {}) is None


def test_update_cells_returns_none_when_open_sheet_fails(monkeypatch):
    gs = _FakeGS("https://example.com/sheet", {"report": []})
    app = _make_app(gs)
    service = SheetsService(app)
    monkeypatch.setattr(service, "open_sheet", lambda _url: False)

    assert service.update_cells("https://example.com/sheet", "report", {"A1": "x"}) is None


def test_update_cells_returns_none_when_sheet_missing():
    gs = _FakeGS("https://example.com/sheet", {"report": []})
    app = _make_app(gs)
    service = SheetsService(app)
    monkeypatch = pytest.MonkeyPatch()
    monkeypatch.setattr(service, "open_sheet", lambda _url: True)

    assert service.update_cells("https://example.com/sheet", "missing", {"A1": "x"}) is None
    monkeypatch.undo()


def test_update_cells_success():
    gs = _FakeGS("https://example.com/sheet", {"report": []})
    app = _make_app(gs)
    service = SheetsService(app)
    monkeypatch = pytest.MonkeyPatch()
    monkeypatch.setattr(service, "open_sheet", lambda _url: True)

    result = service.update_cells("https://example.com/sheet", "report", {"A1": "x", "B2": "y"})
    monkeypatch.undo()

    assert result is True
    assert gs.sheet.updated_cells == {"A1": "x", "B2": "y"}


def test_update_cells_returns_none_on_update_error(monkeypatch):
    gs = _FakeGS("https://example.com/sheet", {"report": []})
    app = _make_app(gs)
    service = SheetsService(app)
    monkeypatch.setattr(service, "open_sheet", lambda _url: True)

    def _raise(_cell, _value):
        raise RuntimeError("acell failed")

    monkeypatch.setattr(gs.sheet._driver, "update_acell", _raise)
    result = service.update_cells("https://example.com/sheet", "report", {"A1": "x"})

    assert result is None


def test_update_range_returns_none_when_values_none():
    gs = _FakeGS("https://example.com/sheet", {"report": []})
    app = _make_app(gs)
    service = SheetsService(app)

    assert service.update_range("https://example.com/sheet", "report", "A1:B2", None) is None


def test_update_range_returns_none_when_open_sheet_fails(monkeypatch):
    gs = _FakeGS("https://example.com/sheet", {"report": []})
    app = _make_app(gs)
    service = SheetsService(app)
    monkeypatch.setattr(service, "open_sheet", lambda _url: False)

    assert service.update_range("https://example.com/sheet", "report", "A1:B2", [["x"]]) is None


def test_update_range_returns_none_when_sheet_missing():
    gs = _FakeGS("https://example.com/sheet", {"report": []})
    app = _make_app(gs)
    service = SheetsService(app)
    monkeypatch = pytest.MonkeyPatch()
    monkeypatch.setattr(service, "open_sheet", lambda _url: True)

    assert service.update_range("https://example.com/sheet", "missing", "A1:B2", [["x"]]) is None
    monkeypatch.undo()


def test_update_range_success():
    gs = _FakeGS("https://example.com/sheet", {"report": []})
    app = _make_app(gs)
    service = SheetsService(app)
    monkeypatch = pytest.MonkeyPatch()
    monkeypatch.setattr(service, "open_sheet", lambda _url: True)

    result = service.update_range("https://example.com/sheet", "report", "A1:B2", [["x", "y"]])
    monkeypatch.undo()

    assert result is True
    assert gs.sheet.updated_ranges["A1:B2"] == [["x", "y"]]


def test_update_range_returns_none_on_update_error(monkeypatch):
    gs = _FakeGS("https://example.com/sheet", {"report": []})
    app = _make_app(gs)
    service = SheetsService(app)
    monkeypatch.setattr(service, "open_sheet", lambda _url: True)

    def _raise(_a1_range, _values):
        raise RuntimeError("range update failed")

    monkeypatch.setattr(gs.sheet._driver, "update", _raise)
    result = service.update_range("https://example.com/sheet", "report", "A1:B2", [["x"]])

    assert result is None
