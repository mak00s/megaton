import pandas as pd
import pytest

from megaton import errors
from megaton.recipes import config_loader


class FakeSheet:
    def __init__(self, data_map):
        self._data_map = data_map
        self._name = None

    def select(self, name):
        if name not in self._data_map:
            raise errors.SheetNotFound
        self._name = name
        return name

    @property
    def data(self):
        if self._name is None:
            return []
        return self._data_map.get(self._name, [])


class FakeGS:
    def __init__(self, data_map):
        self.sheet = FakeSheet(data_map)


class FakeOpen:
    def __init__(self, parent):
        self.parent = parent

    def sheet(self, url):
        self.parent.gs = FakeGS(self.parent._data_map)
        return True


class FakeMG:
    def __init__(self, data_map):
        self._data_map = data_map
        self.gs = None
        self.open = FakeOpen(self)


def test_load_config_builds_maps_and_domains():
    data_map = {
        "config": [
            {
                "clinic": "A",
                "domain": "www.example.com",
                "min_impressions": 10,
                "max_position": 5,
            },
            {"clinic": "B", "domain": "example.org"},
        ],
        "source_map": [
            {"pattern": "^google", "normalized": "google"},
        ],
        "page_map": [
            {"pattern": "/foo", "category": "Foo"},
        ],
        "query_map": [
            {"pattern": "abc", "mapped_to": "ABC"},
        ],
    }
    mg = FakeMG(data_map)

    cfg = config_loader.load_config(mg, "https://example.com/sheet")

    assert cfg.sheet_url == "https://example.com/sheet"
    assert len(cfg.sites) == 2
    assert cfg.source_map == {"^google": "google"}
    assert cfg.page_map == {"/foo": "Foo"}
    assert cfg.query_map == {"abc": "ABC"}
    assert cfg.group_domains == {"example.com", "example.org"}
    assert isinstance(cfg.thresholds_df, pd.DataFrame)
    assert set(cfg.thresholds_df.columns) == {"clinic", "min_impressions", "max_position"}


def test_load_config_raises_on_missing_columns():
    data_map = {
        "config": [{"clinic": "A"}],
        "source_map": [{"pattern": "x"}],
        "page_map": [],
        "query_map": [],
    }
    mg = FakeMG(data_map)

    with pytest.raises(ValueError, match="source_map sheet missing columns"):
        config_loader.load_config(mg, "https://example.com/sheet")


def test_load_config_allows_optional_maps_missing():
    data_map = {
        "config": [{"clinic": "A", "domain": "example.com"}],
        "source_map": [{"pattern": "^google", "normalized": "google"}],
    }
    mg = FakeMG(data_map)

    cfg = config_loader.load_config(mg, "https://example.com/sheet")

    assert cfg.page_map == {}
    assert cfg.query_map == {}
