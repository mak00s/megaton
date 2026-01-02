import pandas as pd
from megaton.start import Megaton


def test_filter_by_thresholds_basic():
    # Create Search instance without full Megaton initialization (avoid auth side-effects)
    search = Megaton.Search(object())

    df = pd.DataFrame(
        {
            "query": ["a", "b", "c", "d"],
            "impressions": [100, 5, 50, 200],
            "position": [1.2, 8.5, 3.0, 10.0],
            "pv": [10, 0, 2, 20],
            "cv": [1, 0, 0, 2],
        }
    )

    site = {"min_impressions": 50, "max_position": 5.0, "min_pv": 1, "min_cv": 1}

    filtered = search.filter_by_thresholds(df, site)
    # only rows meeting all: impressions>=50, position<=5.0, pv>=1, cv>=1
    assert list(filtered["query"]) == ["a"]


def test_filter_by_thresholds_missing_columns():
    search = Megaton.Search(object())

    df = pd.DataFrame({"query": ["x", "y"], "impressions": [10, 100], "position": [2, 3]})
    site = {"min_impressions": 50, "max_position": 5.0}
    filtered = search.filter_by_thresholds(df, site)
    assert list(filtered["query"]) == ["y"]
