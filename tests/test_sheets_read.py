import pandas as pd
import pytest
from types import SimpleNamespace

from megaton.start import Megaton


class _FakeSheet:
    """Minimal fake for gs.sheet with controllable data."""

    def __init__(self, sheets_data: dict):
        self._sheets = sheets_data
        self._name = None

    @property
    def data(self):
        if self._name is None:
            return []
        return self._sheets.get(self._name, [])

    def select(self, name: str):
        if name not in self._sheets:
            raise Exception(f"Sheet '{name}' not found")
        self._name = name
        return name


class _FakeGS:
    """Minimal fake for gs (Google Sheets wrapper)."""

    def __init__(self, sheets_data: dict):
        self._sheets = sheets_data
        self.sheet = _FakeSheet(sheets_data)
        self._url = "https://example.com/sheet"
        self._title = "Fake"

    @property
    def url(self):
        return self._url

    @property
    def title(self):
        return self._title


def _make_app_with_sheet(sheets_data: dict):
    """Create a Megaton instance with a fake gs already opened."""
    app = Megaton()
    gs = _FakeGS(sheets_data)
    app.gs = gs
    app.state.gs_url = gs.url
    app.state.gs_title = gs.title
    return app


class TestSheetsRead:
    """Tests for Sheets.read() shortcut method."""

    def test_read_returns_dataframe_from_list_of_dicts(self):
        app = _make_app_with_sheet({
            "report": [{"month": "2024-01", "pv": 100}, {"month": "2024-02", "pv": 200}],
        })
        result = app.sheets.read("report")

        assert isinstance(result, pd.DataFrame)
        assert len(result) == 2
        assert list(result.columns) == ["month", "pv"]
        assert result.iloc[0]["pv"] == 100

    def test_read_returns_empty_dataframe_for_empty_sheet(self):
        app = _make_app_with_sheet({"empty": []})
        result = app.sheets.read("empty")

        assert isinstance(result, pd.DataFrame)
        assert len(result) == 0

    def test_read_returns_dataframe_when_data_is_already_dataframe(self):
        """If gs.sheet.data returns a DataFrame directly, read() passes it through."""
        app = _make_app_with_sheet({"report": []})
        # Override data property to return a DataFrame
        expected = pd.DataFrame({"a": [1, 2]})
        app.gs.sheet._sheets["report"] = expected
        # Patch data property
        type(app.gs.sheet).__dict__  # ensure class is accessible
        original_data = type(app.gs.sheet).data
        type(app.gs.sheet).data = property(lambda self: expected if self._name == "report" else [])

        try:
            result = app.sheets.read("report")
            assert isinstance(result, pd.DataFrame)
            assert len(result) == 2
        finally:
            type(app.gs.sheet).data = original_data

    def test_read_raises_on_missing_sheet(self):
        app = _make_app_with_sheet({"existing": [{"a": 1}]})

        with pytest.raises(Exception, match="not found"):
            app.sheets.read("nonexistent")

    def test_read_updates_state(self):
        app = _make_app_with_sheet({
            "report": [{"a": 1}],
        })
        app.sheets.read("report")

        assert app.state.gs_sheet_name == "report"
