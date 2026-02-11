import types

import pytest
from google.cloud.exceptions import NotFound

from megaton import bq


class _DummyTableRow:
    def __init__(self, table_id: str):
        self.table_id = table_id


class _DummyDatasetRef:
    def table(self, table_id: str):
        return f"ref:{table_id}"


class _DummyDatasetObj:
    def __init__(self):
        self.reference = _DummyDatasetRef()


class _DummyClient:
    def __init__(self):
        self._datasets = {"ds1": _DummyDatasetObj()}
        self._tables = {"ref:t1": object()}

    def get_dataset(self, dataset_id: str):
        if dataset_id not in self._datasets:
            raise NotFound("not found")
        return self._datasets[dataset_id]

    def list_tables(self, dataset):
        return [_DummyTableRow("t1")]

    def get_table(self, table_ref):
        if table_ref not in self._tables:
            raise NotFound("not found")
        return self._tables[table_ref]


def _make_bq_parent():
    parent = types.SimpleNamespace()
    parent.id = "proj1"
    parent.client = _DummyClient()
    parent.parent = types.SimpleNamespace(state=types.SimpleNamespace())
    parent.datasets = ["ds1"]
    parent.update = lambda: True
    parent.dataset = bq.MegatonBQ.Dataset(parent)
    parent.table = bq.MegatonBQ.Table(parent)
    return parent


def test_dataset_select_missing_raises_value_error():
    parent = _make_bq_parent()

    with pytest.raises(ValueError):
        parent.dataset.select("missing")


def test_dataset_update_without_selection_raises_value_error():
    parent = _make_bq_parent()

    with pytest.raises(ValueError):
        parent.dataset.update()


def test_dataset_select_updates_state_and_returns_true():
    parent = _make_bq_parent()

    assert parent.dataset.select("ds1") is True
    assert parent.dataset.id == "ds1"
    assert parent.parent.state.bq_dataset_id == "ds1"
    assert parent.dataset.tables == ["t1"]


def test_table_select_without_dataset_raises_value_error():
    parent = _make_bq_parent()

    with pytest.raises(ValueError):
        parent.table.select("t1")


def test_table_update_without_selection_raises_value_error():
    parent = _make_bq_parent()
    parent.dataset.select("ds1")

    with pytest.raises(ValueError):
        parent.table.update()


def test_table_select_updates_state_and_returns_true():
    parent = _make_bq_parent()
    parent.dataset.select("ds1")

    assert parent.table.select("t1") is True
    assert parent.table.id == "t1"
    assert parent.parent.state.bq_table_id == "t1"


def test_table_create_without_dataset_raises_value_error():
    parent = _make_bq_parent()

    with pytest.raises(ValueError):
        parent.table.create("new_table", schema=[])
