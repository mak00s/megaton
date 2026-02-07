from datetime import datetime
from types import SimpleNamespace

import pandas as pd

from megaton import ga4


def _parent_with_property(property_id="123"):
    return SimpleNamespace(
        property=SimpleNamespace(
            id=property_id,
            created_time=datetime(2024, 1, 1),
            api_metadata={
                "dimensions": [
                    {"api_name": "date", "display_name": "Date"},
                    {"api_name": "country", "display_name": "Country"},
                ],
                "metrics": [
                    {"api_name": "eventCount", "display_name": "Event count"},
                    {"api_name": "totalUsers", "display_name": "Total users"},
                ],
            },
        ),
        data_client=SimpleNamespace(),
    )


def test_parse_operator_metric_variants():
    report = ga4.MegatonGA4.Report(_parent_with_property())

    assert report._parse_operator("==", "metrics").name == "EQUAL"
    assert report._parse_operator(">", "metrics").name == "GREATER_THAN"
    assert report._parse_operator("<", "metrics").name == "LESS_THAN"
    assert report._parse_operator("~", "metrics").name == "OPERATION_UNSPECIFIED"


def test_format_filter_multiple_conditions_builds_and_group():
    report = ga4.MegatonGA4.Report(_parent_with_property())

    expr = report._format_filter("date==2024-01-01;eventCount>10")

    assert expr.and_group is not None
    assert len(expr.and_group.expressions) == 2
    assert expr.and_group.expressions[0].filter.field_name == "date"
    assert expr.and_group.expressions[1].filter.field_name == "eventCount"


def test_convert_metric_numeric_and_fallback():
    report = ga4.MegatonGA4.Report(_parent_with_property())

    assert report._convert_metric("10", "TYPE_INTEGER") == 10
    assert report._convert_metric("1.5", "TYPE_FLOAT") == 1.5
    assert report._convert_metric("not-number", "TYPE_INTEGER") == "not-number"


def test_parse_response_converts_metric_values():
    report = ga4.MegatonGA4.Report(_parent_with_property())

    response = SimpleNamespace(
        dimension_headers=[SimpleNamespace(name="date")],
        metric_headers=[SimpleNamespace(name="eventCount", type_=1)],
        rows=[
            SimpleNamespace(
                dimension_values=[SimpleNamespace(value="2024-01-01")],
                metric_values=[SimpleNamespace(value="7")],
            )
        ],
    )

    data, headers, types = report._parse_response(response)

    assert data == [["2024-01-01", 7]]
    assert headers == ["date", "eventCount"]
    assert types == ["category", "TYPE_INTEGER"]


def test_run_returns_none_without_property_id():
    report = ga4.MegatonGA4.Report(_parent_with_property(property_id=None))

    assert report.run(dimensions=["date"], metrics=["eventCount"]) is None


def test_run_paginates_and_returns_dataframe(monkeypatch):
    report = ga4.MegatonGA4.Report(_parent_with_property())
    offsets = []

    monkeypatch.setattr(report, "_format_request", lambda **_kwargs: SimpleNamespace())

    def _fake_request(offset, _request, **_kwargs):
        offsets.append(offset)
        if offset == 0:
            return (
                [["2024-01-01", 1], ["2024-01-02", 2]],
                3,
                ["date", "eventCount"],
                ["category", "TYPE_INTEGER"],
            )
        if offset == 2:
            return (
                [["2024-01-03", 3]],
                3,
                ["date", "eventCount"],
                ["category", "TYPE_INTEGER"],
            )
        return (
            [],
            3,
            ["date", "eventCount"],
            ["category", "TYPE_INTEGER"],
        )

    monkeypatch.setattr(report, "_request_report_api", _fake_request)
    df = report.run(dimensions=["date"], metrics=["eventCount"], limit=2)

    assert offsets == [0, 2]
    assert isinstance(df, pd.DataFrame)
    assert len(df) == 3
    assert list(df.columns) == ["date", "eventCount"]


def test_run_trims_dimensions_and_metrics_before_request(monkeypatch):
    report = ga4.MegatonGA4.Report(_parent_with_property())
    captured = {}

    def _capture_request(**kwargs):
        captured["dimensions"] = kwargs["dimensions"]
        captured["metrics"] = kwargs["metrics"]
        return SimpleNamespace()

    monkeypatch.setattr(report, "_format_request", _capture_request)
    monkeypatch.setattr(report, "_request_report_api", lambda *_args, **_kwargs: ([], 0, [], []))

    dims = [f"d{i}" for i in range(11)]
    metrics = [f"m{i}" for i in range(12)]
    df = report.run(dimensions=dims, metrics=metrics, limit=10)

    assert isinstance(df, pd.DataFrame)
    assert df.empty
    assert len(captured["dimensions"]) == 9
    assert len(captured["metrics"]) == 10


def test_convert_ga4_type_to_bq_type_mapping():
    assert ga4.convert_ga4_type_to_bq_type("string") == "STRING"
    assert ga4.convert_ga4_type_to_bq_type("int") == "INT64"
    assert ga4.convert_ga4_type_to_bq_type("integer") == "INT64"
    assert ga4.convert_ga4_type_to_bq_type("float") == "FLOAT"
    assert ga4.convert_ga4_type_to_bq_type("double") == "FLOAT"
    assert ga4.convert_ga4_type_to_bq_type("other") is None


def test_convert_proto_datetime_supports_seconds_and_timestamp():
    class _WithSeconds:
        seconds = 0

    class _WithTimestamp:
        def timestamp(self):
            return 0

    dt1 = ga4.convert_proto_datetime(_WithSeconds())
    dt2 = ga4.convert_proto_datetime(_WithTimestamp())

    assert dt1.year == 1970
    assert dt2.year == 1970
