from datetime import datetime
from types import SimpleNamespace

import pandas as pd
import pytest

from megaton import errors, ga4


def _report_parent(property_id="123"):
    return SimpleNamespace(
        property=SimpleNamespace(
            id=property_id,
            created_time=datetime(2024, 1, 1),
            api_metadata={
                "dimensions": [
                    {"api_name": "date", "display_name": "Date"},
                ],
                "metrics": [
                    {"api_name": "eventCount", "display_name": "Event count"},
                ],
            },
        ),
        data_client=SimpleNamespace(),
    )


def test_account_select_updates_state_and_clear_resets_fields():
    property_cleared = {"called": 0}
    state = SimpleNamespace(ga_version=None, ga_account_id=None, ga_property_id="p1", ga_view_id="v1")
    parent = SimpleNamespace(
        property=SimpleNamespace(clear=lambda: property_cleared.__setitem__("called", 1)),
        _state=state,
        _ga_version="GA4",
    )
    account = ga4.MegatonGA4.Account(parent)

    account._update = lambda: [{"id": "acc1"}]
    account.select("acc1")

    assert account.id == "acc1"
    assert state.ga_version == "GA4"
    assert state.ga_account_id == "acc1"

    account.select("")

    assert account.id is None
    assert property_cleared["called"] == 1
    assert state.ga_account_id is None
    assert state.ga_property_id is None
    assert state.ga_view_id is None


def test_property_select_updates_and_clear_resets_state():
    state = SimpleNamespace(ga_version=None, ga_property_id=None, ga_view_id="v1")
    parent = SimpleNamespace(_state=state, _ga_version="GA4")
    prop = ga4.MegatonGA4.Property(parent)

    called = {"update": 0}
    prop._update = lambda: called.__setitem__("update", called["update"] + 1)

    prop.select("123")
    prop.select("123")

    assert called["update"] == 1
    assert state.ga_property_id == "123"

    prop.select(None)

    assert prop.id is None
    assert state.ga_property_id is None
    assert state.ga_view_id is None


def test_report_set_dates_strips_whitespace():
    report = ga4.MegatonGA4.Report(_report_parent())

    report.set_dates(" 2024-01-01 ", " 2024-01-31 ")

    assert report.start_date == "2024-01-01"
    assert report.end_date == "2024-01-31"


def test_format_name_raises_bad_request_on_unknown_field():
    report = ga4.MegatonGA4.Report(_report_parent())

    with pytest.raises(errors.BadRequest):
        report._format_name("unknownField")


def test_parse_filter_condition_builds_dimension_and_metric_filters():
    report = ga4.MegatonGA4.Report(_report_parent())

    dim_expr = report._parse_filter_condition("Date", "=@", "2024")
    metric_expr = report._parse_filter_condition("Event count", "!=", "10")

    assert dim_expr.filter.field_name == "date"
    assert dim_expr.filter.string_filter.value == "2024"
    assert metric_expr.not_expression.filter.field_name == "eventCount"
    assert metric_expr.not_expression.filter.numeric_filter.value.int64_value == 10


def test_run_to_pd_false_returns_raw_rows_headers_types(monkeypatch):
    report = ga4.MegatonGA4.Report(_report_parent())

    monkeypatch.setattr(report, "_format_request", lambda **_kwargs: SimpleNamespace())
    monkeypatch.setattr(
        report,
        "_request_report_api",
        lambda *_args, **_kwargs: ([['2024-01-01', 7]], 1, ['date', 'eventCount'], ['category', 'TYPE_INTEGER']),
    )

    rows, headers, types = report.run(dimensions=["date"], metrics=["eventCount"], to_pd=False)

    assert rows == [["2024-01-01", 7]]
    assert headers == ["date", "eventCount"]
    assert types == ["category", "TYPE_INTEGER"]


def test_run_to_pd_false_with_no_data_returns_empty_rows(monkeypatch):
    report = ga4.MegatonGA4.Report(_report_parent())

    monkeypatch.setattr(report, "_format_request", lambda **_kwargs: SimpleNamespace())
    monkeypatch.setattr(report, "_request_report_api", lambda *_args, **_kwargs: ([], 0, ["date"], ["category"]))

    rows, headers, types = report.run(dimensions=["date"], metrics=["eventCount"], to_pd=False)

    assert rows == []
    assert headers == ["date"]
    assert types == ["category"]


def test_run_with_data_to_dataframe_still_works(monkeypatch):
    report = ga4.MegatonGA4.Report(_report_parent())

    monkeypatch.setattr(report, "_format_request", lambda **_kwargs: SimpleNamespace())
    monkeypatch.setattr(
        report,
        "_request_report_api",
        lambda *_args, **_kwargs: ([['2024-01-01', 2]], 1, ['date', 'eventCount'], ['category', 'TYPE_INTEGER']),
    )

    df = report.run(dimensions=["date"], metrics=["eventCount"], to_pd=True)

    assert isinstance(df, pd.DataFrame)
    assert list(df.columns) == ["date", "eventCount"]
    assert df.iloc[0]["eventCount"] == 2
