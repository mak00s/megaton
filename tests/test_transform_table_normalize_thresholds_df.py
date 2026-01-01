import pandas as pd

from megaton.transform import table


def test_normalize_thresholds_df_none_returns_none():
    assert table.normalize_thresholds_df(None) is None


def test_normalize_thresholds_df_adds_missing_columns():
    df = pd.DataFrame({"extra": [1, 2]})
    result = table.normalize_thresholds_df(df)
    assert list(result.columns)[:3] == ["clinic", "min_impressions", "max_position"]
    assert result["min_impressions"].tolist() == [10, 10]
    assert result["max_position"].tolist() == [50, 50]
    assert result["clinic"].tolist() == [None, None]
    assert "extra" in result.columns


def test_normalize_thresholds_df_coerces_and_fills_defaults():
    df = pd.DataFrame(
        {
            "clinic": ["A", "B", "C", "D", "E"],
            "min_impressions": ["10", " 20 ", "", None, "abc"],
            "max_position": ["5", "", None, "50", "xyz"],
        }
    )
    result = table.normalize_thresholds_df(df)
    assert result["min_impressions"].tolist() == [10, 20, 10, 10, 10]
    assert result["max_position"].tolist() == [5, 50, 50, 50, 50]


def test_normalize_thresholds_df_keeps_extra_columns():
    df = pd.DataFrame({"clinic": ["A"], "min_impressions": [1], "max_position": [2], "note": ["x"]})
    result = table.normalize_thresholds_df(df)
    assert "note" in result.columns
    assert result.loc[0, "note"] == "x"


def test_normalize_thresholds_df_empty_dataframe():
    df = pd.DataFrame(columns=["clinic"])
    result = table.normalize_thresholds_df(df)
    assert list(result.columns)[:3] == ["clinic", "min_impressions", "max_position"]
    assert len(result) == 0
