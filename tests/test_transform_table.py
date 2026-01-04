import pandas as pd

from megaton.transform import table


def test_ensure_columns_adds_missing_and_orders():
    df = pd.DataFrame({"a": [1], "b": [2]})
    result = table.ensure_columns(df, ["a", "b", "c"], fill=0)
    assert list(result.columns) == ["a", "b", "c"]
    assert result.loc[0, "c"] == 0


def test_normalize_key_cols_strips_and_removes_dot0():
    df = pd.DataFrame({"k": ["1.0", " 2 ", "3"], "v": [1, 2, 3]})
    result = table.normalize_key_cols(df, ["k"])
    assert result["k"].tolist() == ["1", "2", "3"]


def test_dedup_by_key_prefers_highest_value():
    df = pd.DataFrame({"k": [1, 1, 2], "score": [5, 9, 1], "val": [10, 20, 30]})
    # デフォルト（prefer_ascending=False）は最大値を選択
    result = table.dedup_by_key(df, ["k"], prefer_by="score")
    assert len(result) == 2
    assert result.loc[result["k"] == 1, "val"].iloc[0] == 20
    
    # prefer_ascending=True は最小値を選択
    result_min = table.dedup_by_key(df, ["k"], prefer_by="score", prefer_ascending=True)
    assert len(result_min) == 2
    assert result_min.loc[result_min["k"] == 1, "val"].iloc[0] == 10


def test_group_sum_sums_columns():
    df = pd.DataFrame({"g": ["a", "a", "b"], "x": [1, 2, 3], "y": [10, 20, 30]})
    result = table.group_sum(df, ["g"], ["x", "y"])
    result = result.sort_values("g").reset_index(drop=True)
    assert result.loc[0, "x"] == 3
    assert result.loc[0, "y"] == 30
    assert result.loc[1, "x"] == 3
    assert result.loc[1, "y"] == 30


def test_weighted_avg_calculates_weighted_value():
    df = pd.DataFrame({"g": ["a", "a", "b"], "val": [10, 20, 100], "w": [1, 3, 2]})
    result = table.weighted_avg(df, ["g"], "val", "w")
    result = result.sort_values("g").reset_index(drop=True)
    assert result.loc[0, "val"] == 17.5
    assert result.loc[1, "val"] == 100.0
