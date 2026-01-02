import pandas as pd
from megaton.start import start


def test_filter_by_thresholds_basic():
    # create a dummy Search instance by instantiating Megaton and accessing .search
    class DummyParent:
        pass

    # Instead of creating full Megaton, import the Search class directly
    from megaton.start import start as start_mod
    # Create a minimal parent structure required by Search
    class P:
        def __init__(self):
            self._gsc_service = None

    parent = P()
    S = start_mod.Megaton  # not used, but import ensures module loaded

    from megaton.start import Megaton
    # instantiate Search without initializing Megaton (avoid auth side-effects)
    parent = object()
    search = Megaton.Search(parent)

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
    from megaton.start import Megaton
    parent = object()
    search = Megaton.Search(parent)

    df = pd.DataFrame({"query": ["x", "y"], "impressions": [10, 100], "position": [2, 3]})
    site = {"min_impressions": 50, "max_position": 5.0}
    filtered = search.filter_by_thresholds(df, site)
    assert list(filtered["query"]) == ["y"]
