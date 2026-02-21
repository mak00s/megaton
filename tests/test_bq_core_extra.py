from types import SimpleNamespace

import pytest
from google.cloud.exceptions import NotFound

from megaton import bq


class _DatasetRow:
    def __init__(self, dataset_id: str):
        self.dataset_id = dataset_id


class _TableRow:
    def __init__(self, table_id: str):
        self.table_id = table_id


class _DatasetRef:
    def __init__(self, dataset_id: str):
        self.dataset_id = dataset_id

    def table(self, table_id: str):
        return f"{self.dataset_id}.{table_id}"


class _DatasetObj:
    def __init__(self, dataset_id: str):
        self.dataset_id = dataset_id
        self.reference = _DatasetRef(dataset_id)


class _QueryResults:
    def __init__(self, df_result):
        self._df_result = df_result

    def to_dataframe(self):
        return self._df_result


class _QueryJob:
    def __init__(self, results):
        self._results = results

    def result(self):
        return self._results


class _Client:
    def __init__(self):
        self.datasets = [_DatasetRow("ds1")]
        self.tables = {"ds1": [_TableRow("t1")]}
        self.dataset_objs = {"ds1": _DatasetObj("ds1")}
        self.table_objs = {"ds1.t1": SimpleNamespace(table_id="t1", time_partitioning=SimpleNamespace(field=None))}
        self.created_tables = []

    def list_datasets(self):
        return self.datasets

    def query(self, query: str):
        return _QueryJob(_QueryResults({"query": query}))

    def get_dataset(self, dataset_id: str):
        if dataset_id not in self.dataset_objs:
            raise NotFound("missing")
        return self.dataset_objs[dataset_id]

    def list_tables(self, dataset):
        return self.tables.get(dataset.dataset_id, [])

    def get_table(self, table_ref):
        if table_ref not in self.table_objs:
            raise NotFound("missing")
        return self.table_objs[table_ref]

    def create_table(self, table):
        self.created_tables.append(table)
        if not getattr(table, "table_id", None):
            table.table_id = table.table_ref.split(".")[-1]
        if not hasattr(table, "time_partitioning"):
            table.time_partitioning = SimpleNamespace(field=None)
        return table


class _FakeTable:
    def __init__(self, table_ref, schema=None):
        self.table_ref = table_ref
        self.table_id = str(table_ref).split(".")[-1]
        self.schema = schema
        self.time_partitioning = SimpleNamespace(field=None)
        self.clustering_fields = None
        self.description = None


class _FakeTimePartitioning:
    def __init__(self, type_, field):
        self.type_ = type_
        self.field = field


def _make_parent_state():
    return SimpleNamespace(state=SimpleNamespace(bq_dataset_id="x", bq_table_id="y"))


def test_init_builds_client_and_refreshes_datasets(monkeypatch):
    fake_client = _Client()

    monkeypatch.setattr(bq.bigquery, "Client", lambda project, credentials: fake_client)

    obj = bq.MegatonBQ(parent=SimpleNamespace(), credentials=object(), project_id="proj")

    assert obj.id == "proj"
    assert obj.datasets == ["ds1"]
    assert obj.client is fake_client


def test_update_with_no_datasets_sets_empty_list(capsys):
    obj = bq.MegatonBQ.__new__(bq.MegatonBQ)
    obj.id = "proj"
    obj.client = _Client()
    obj.client.datasets = []

    assert obj.update() is True
    assert obj.datasets == []
    assert "has no datasets" in capsys.readouterr().out


def test_run_returns_raw_result_or_dataframe():
    obj = bq.MegatonBQ.__new__(bq.MegatonBQ)
    obj.client = _Client()

    raw = obj.run("SELECT 1", to_dataframe=False)
    as_df = obj.run("SELECT 2", to_dataframe=True)

    assert isinstance(raw, _QueryResults)
    assert as_df == {"query": "SELECT 2"}


def test_dataset_select_clear_resets_state_and_table_selection():
    selected = []
    parent = SimpleNamespace(parent=_make_parent_state(), table=SimpleNamespace(select=lambda: selected.append(True)))
    dataset = bq.MegatonBQ.Dataset(parent)
    dataset.id = "ds1"
    dataset.ref = object()

    assert dataset.select(None) is True
    assert dataset.id is None
    assert parent.parent.state.bq_dataset_id is None
    assert parent.parent.state.bq_table_id is None
    assert selected == [True]


def test_dataset_select_same_id_short_circuits_without_update(monkeypatch):
    parent = SimpleNamespace(datasets=["ds1"], id="proj")
    dataset = bq.MegatonBQ.Dataset(parent)
    dataset.id = "ds1"
    dataset.ref = object()

    called = {"update": 0}
    monkeypatch.setattr(dataset, "update", lambda *_args, **_kwargs: called.__setitem__("update", 1))

    assert dataset.select("ds1") is True
    assert called["update"] == 0


def test_dataset_update_not_found_maps_to_value_error():
    parent = SimpleNamespace(id="proj", client=_Client(), parent=SimpleNamespace(), table=SimpleNamespace(select=lambda: True))
    dataset = bq.MegatonBQ.Dataset(parent)

    with pytest.raises(ValueError, match="Dataset 'missing'"):
        dataset.update("missing")


def test_table_select_clear_and_update_not_found():
    parent = SimpleNamespace(parent=_make_parent_state())
    parent.dataset = SimpleNamespace(ref=_DatasetRef("ds1"), id="ds1", tables=["t1"], update=lambda _id: True)
    parent.client = _Client()
    table = bq.MegatonBQ.Table(parent)
    table.id = "t1"
    table.ref = object()

    assert table.select(None) is True
    assert table.id is None
    assert parent.parent.state.bq_table_id is None

    with pytest.raises(ValueError, match="Table 'missing'"):
        table.update("missing")


def test_table_create_applies_options_and_refreshes_dataset(monkeypatch):
    parent = SimpleNamespace()
    parent.client = _Client()
    refreshed = []
    parent.dataset = SimpleNamespace(ref=_DatasetRef("ds1"), update=lambda *_args: refreshed.append(True))

    table = bq.MegatonBQ.Table(parent)

    monkeypatch.setattr(bq.bigquery, "Table", _FakeTable)
    monkeypatch.setattr(bq.bigquery, "TimePartitioning", _FakeTimePartitioning)
    monkeypatch.setattr(bq.bigquery, "TimePartitioningType", SimpleNamespace(DAY="DAY"))

    created = table.create(
        table_id="events",
        schema=[],
        description="daily events",
        partitioning_field="date",
        clustering_fields=["date", "source"],
    )

    assert created.table_id == "events"
    assert created.time_partitioning.field == "date"
    assert created.clustering_fields == ["date", "source"]
    assert created.description == "daily events"
    assert refreshed == [True]
