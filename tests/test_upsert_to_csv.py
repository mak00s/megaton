from types import SimpleNamespace

import pandas as pd
import pytest

from megaton.start import Megaton


def test_upsert_to_csv_creates_file_when_missing(tmp_path):
    app = Megaton(None, headless=True)
    df = pd.DataFrame([{"id": "a", "value": 1}])

    result = app.upsert.to.csv(
        df,
        filename=str(tmp_path / "report"),
        keys=["id"],
        include_dates=False,
        quiet=True,
    )

    saved = pd.read_csv(tmp_path / "report.csv")
    pd.testing.assert_frame_equal(result.reset_index(drop=True), df.reset_index(drop=True))
    pd.testing.assert_frame_equal(saved.reset_index(drop=True), df.reset_index(drop=True))


def test_upsert_to_csv_deduplicates_and_sorts(tmp_path):
    app = Megaton(None, headless=True)
    filename = tmp_path / "report.csv"
    pd.DataFrame(
        [
            {"id": "a", "value": 1},
            {"id": "b", "value": 2},
        ]
    ).to_csv(filename, index=False)

    df_new = pd.DataFrame(
        [
            {"id": "b", "value": 20},
            {"id": "c", "value": 3},
        ]
    )

    result = app.upsert.to.csv(
        df_new,
        filename=str(tmp_path / "report"),
        keys=["id"],
        sort_by=["id"],
        include_dates=False,
        quiet=True,
    )

    expected = pd.DataFrame(
        [
            {"id": "a", "value": 1},
            {"id": "b", "value": 20},
            {"id": "c", "value": 3},
        ]
    )
    saved = pd.read_csv(filename)
    pd.testing.assert_frame_equal(result.reset_index(drop=True), expected.reset_index(drop=True))
    pd.testing.assert_frame_equal(saved.reset_index(drop=True), expected.reset_index(drop=True))


def test_upsert_to_csv_applies_columns(tmp_path):
    app = Megaton(None, headless=True)
    filename = tmp_path / "report.csv"
    pd.DataFrame([{"id": "a", "value": 1, "note": "x"}]).to_csv(filename, index=False)

    df_new = pd.DataFrame([{"id": "a", "value": 2}])
    result = app.upsert.to.csv(
        df_new,
        filename=str(tmp_path / "report"),
        keys=["id"],
        columns=["id", "value", "extra"],
        include_dates=False,
        quiet=True,
    )

    assert list(result.columns) == ["id", "value", "extra"]
    saved = pd.read_csv(filename)
    assert list(saved.columns) == ["id", "value", "extra"]


def test_upsert_to_csv_uses_dates_in_filename_and_quiet(tmp_path, capsys):
    app = Megaton(None, headless=True)
    app.ga = {"4": SimpleNamespace(report=SimpleNamespace(start_date=None, end_date=None))}
    app.state.ga_version = "4"
    app.report.set.dates("2024-01-01", "2024-01-31")

    df = pd.DataFrame([{"id": "a", "value": 1}])
    app.upsert.to.csv(
        df,
        filename=str(tmp_path / "report"),
        keys=["id"],
        include_dates=True,
        quiet=True,
    )

    captured = capsys.readouterr()
    assert captured.out == ""
    assert (tmp_path / "report_20240101-20240131.csv").exists()


def test_upsert_to_csv_rejects_invalid_df():
    app = Megaton(None, headless=True)

    with pytest.raises(TypeError, match="pandas DataFrame"):
        app.upsert.to.csv("not-a-df", keys=["id"])
