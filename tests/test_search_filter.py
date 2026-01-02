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


def test_filter_by_thresholds_clicks_zero_only():
    """Test that clicks_zero_only parameter filters only zero-click rows."""
    search = Megaton.Search(object())

    # Test data matching problem statement scenario
    df = pd.DataFrame(
        {
            "query": ["a", "b", "c"],
            "impressions": [100, 5, 50],
            "position": [1.0, 10.0, 3.0],
            "clicks": [1, 0, 0],
        }
    )

    site = {"min_impressions": 50, "max_position": 5}

    # Default behavior (clicks_zero_only=False): apply thresholds to all rows
    filtered_no_flag = search.filter_by_thresholds(df, site)
    # "a": impressions=100>=50 ✓, position=1.0<=5 ✓ → KEPT
    # "b": impressions=5<50 ✗ → EXCLUDED
    # "c": impressions=50>=50 ✓, position=3.0<=5 ✓ → KEPT
    assert sorted(list(filtered_no_flag["query"])) == ["a", "c"]

    # With clicks_zero_only=True: preserve rows with clicks>0, filter only clicks==0 rows
    filtered_zero_flag = search.filter_by_thresholds(df, site, clicks_zero_only=True)
    # "a": clicks=1>0 → KEPT unconditionally (regardless of thresholds)
    # "b": clicks=0, impressions=5<50 → EXCLUDED by threshold
    # "c": clicks=0, impressions=50>=50 ✓, position=3.0<=5 ✓ → KEPT
    assert sorted(list(filtered_zero_flag["query"])) == ["a", "c"]

    # Additional test: verify that clicks>0 rows are kept even if they fail thresholds
    df2 = pd.DataFrame(
        {
            "query": ["x", "y"],
            "impressions": [10, 100],  # "x" fails min_impressions
            "position": [2.0, 3.0],
            "clicks": [5, 0],  # "x" has clicks>0
        }
    )
    
    filtered_default = search.filter_by_thresholds(df2, site)
    # Default: only "y" passes thresholds
    assert list(filtered_default["query"]) == ["y"]
    
    filtered_clicks_zero = search.filter_by_thresholds(df2, site, clicks_zero_only=True)
    # clicks_zero_only: "x" kept despite failing threshold (because clicks>0), "y" kept (passes threshold)
    assert sorted(list(filtered_clicks_zero["query"])) == ["x", "y"]
