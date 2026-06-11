"""Tests for chain-API canonicalization pieces (1.4.0):

- megaton.wrap(df) entry point
- Save/Append/Upsert/_coerce_df accepting Result objects
- ReportResult.month_key()
- transform.table.fillna_int
"""
import pandas as pd
import pytest

import megaton
from megaton.start import Megaton, ReportResult, _extract_df, wrap
from megaton.transform import fillna_int


def _df():
    return pd.DataFrame({
        "date": ["20260101", "20260102", "20260215"],
        "channel": ["Organic", "Paid", "Organic"],
        "sessions": [10, 20, 30],
    })


class TestWrap:
    def test_wrap_returns_chainable_result(self):
        result = wrap(_df())
        assert isinstance(result, ReportResult)
        out = result.group("channel").to_int().sort("channel")
        assert out["sessions"].tolist() == [40, 20]

    def test_wrap_infers_non_numeric_dimensions(self):
        assert wrap(_df()).dimensions == ["date", "channel"]

    def test_wrap_explicit_dimensions(self):
        assert wrap(_df(), dimensions=["channel"]).dimensions == ["channel"]

    def test_wrap_copies_source(self):
        src = _df()
        wrap(src).fill(to="x")
        assert src.loc[0, "date"] == "20260101"

    def test_wrap_rejects_non_dataframe(self):
        with pytest.raises(TypeError):
            wrap([1, 2, 3])

    def test_lazy_module_export(self):
        from megaton import start as start_module

        # Compare against current module attributes (other tests may patch
        # start.Megaton, so don't compare against import-time references).
        assert megaton.wrap is start_module.wrap
        assert megaton.Megaton is start_module.Megaton


class TestMonthKey:
    def test_default_overwrites_dimension(self):
        out = wrap(_df()).month_key("date", fmt="%Y-%m")
        assert out["date"].tolist() == ["2026-01", "2026-01", "2026-02"]

    def test_into_adds_new_dimension(self):
        out = wrap(_df()).month_key("date", into="month", fmt="%Y%m")
        assert out["month"].tolist() == ["202601", "202601", "202602"]
        assert "month" in out.dimensions

    def test_chain_month_aggregation(self):
        out = (
            wrap(_df())
            .month_key("date", into="month", fmt="%Y-%m")
            .group("month")
            .to_int()
        )
        assert out["sessions"].tolist() == [30, 30]
        assert out["month"].tolist() == ["2026-01", "2026-02"]

    def test_missing_column_raises(self):
        with pytest.raises(KeyError):
            wrap(_df()).month_key("nope")


class TestExtractDf:
    def test_unwraps_report_result(self):
        result = wrap(_df())
        assert _extract_df(result) is result.df or _extract_df(result).equals(result.df)

    def test_passthrough(self):
        df = _df()
        assert _extract_df(df) is df
        assert _extract_df(None) is None


class TestSaveAcceptsResult:
    def test_save_to_csv_accepts_result(self, tmp_path, monkeypatch):
        app = Megaton(None, headless=True)
        monkeypatch.chdir(tmp_path)
        result = wrap(_df())
        app.save.to.csv(result, filename="out", include_dates=False, quiet=True)
        saved = pd.read_csv(tmp_path / "out.csv")
        assert len(saved) == 3

    def test_append_to_csv_accepts_result(self, tmp_path, monkeypatch):
        app = Megaton(None, headless=True)
        monkeypatch.chdir(tmp_path)
        app.save.to.csv(wrap(_df()), filename="out", include_dates=False, quiet=True)
        app.append.to.csv(wrap(_df()), filename="out.csv", include_dates=False, quiet=True)
        saved = pd.read_csv(tmp_path / "out.csv")
        assert len(saved) == 6


class TestFillnaInt:
    def test_fills_and_converts(self):
        df = pd.DataFrame({"a": [1.0, None], "b": ["2", "x"], "keep": ["s", "t"]})
        out = fillna_int(df, ["a", "b", "missing"])
        assert out["a"].tolist() == [1, 0]
        assert out["b"].tolist() == [2, 0]
        assert out["keep"].tolist() == ["s", "t"]
