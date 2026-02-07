"""Tests for Report.prep() behavior."""
from unittest.mock import patch

import pandas as pd

from megaton.start import Megaton


def test_report_prep_uses_report_data_when_df_none():
    """Report.prep() should use and update report.data when df is omitted."""
    mg = Megaton()
    df = pd.DataFrame({"a": [1], "b": [2]})
    mg.report.data = df

    if hasattr(mg, "data"):
        delattr(mg, "data")

    conf = {"a": {"name": "alpha"}}

    with patch.object(mg.show, "table", return_value="ok") as mock_show:
        result = mg.report.prep(conf)

    assert result == "ok"
    assert "alpha" in mg.report.data.columns
    mock_show.assert_called_once()


def test_report_prep_show_false_suppresses_display_and_returns_df():
    mg = Megaton()
    mg.report.data = pd.DataFrame({"a": [1], "b": [2]})
    conf = {"a": {"name": "alpha"}}

    with patch.object(mg.show, "table", return_value="ok") as mock_show:
        result = mg.report.prep(conf, show=False)

    assert isinstance(result, pd.DataFrame)
    assert "alpha" in result.columns
    assert mg.report.data is result
    mock_show.assert_not_called()
