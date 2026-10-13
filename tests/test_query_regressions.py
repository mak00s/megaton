"""Exercise real execution layers instead of mocking report.run itself."""

import io
import json
from datetime import datetime
from types import SimpleNamespace
from unittest.mock import Mock

import pandas as pd
import pytest

from megaton import cli, errors
from megaton.ga4 import MegatonGA4
from megaton.query import (
    QueryAuthenticationError,
    QueryValidationError,
    list_resources,
    run_query,
    validate_query,
)
from megaton.services.gsc_service import GSCService
from megaton.start import Megaton


def params(source="ga4", **changes):
    value = {
        "schema_version": "1.0", "source": source,
        "property_id" if source == "ga4" else "site_url": "123" if source == "ga4" else "sc-domain:example.com",
        "date_range": {"start": "2026-01-01", "end": "2026-01-31"},
        "dimensions": ["date"], **({"metrics": ["sessions"]} if source == "ga4" else {}),
    }
    return {**value, **changes}


def session():
    parent = SimpleNamespace(property=SimpleNamespace(
        id="123", created_time=datetime(2024, 1, 1),
        api_metadata={"dimensions": [{"api_name": "date", "display_name": "Date"}],
                      "metrics": [{"api_name": "sessions", "display_name": "Sessions"}]},
    ))
    backend = MegatonGA4.Report(parent)
    backend._request_report_api = Mock(return_value=([["20260101", 3]], 1, ["date", "sessions"], []))
    mg = Megaton.__new__(Megaton)
    mg.ga = {"4": SimpleNamespace(report=backend)}
    mg._retry = {}
    mg.use_property = Mock()
    mg.report = Megaton.Report(mg)
    return mg, backend


def test_reused_session_badrequest_cannot_return_previous_rows():
    mg, backend = session()
    assert len(run_query(params(), client=mg).df) == 1
    with pytest.raises(errors.BadRequest):
        run_query(params(filter_d="unknownField==bad"), client=mg)
    assert mg.report.data is None
    assert backend._request_report_api.call_count == 1


@pytest.mark.parametrize("sort", ["-sessions,date", ["-sessions", "date"]])
def test_sort_reaches_real_ga4_request(sort):
    mg, backend = session()
    run_query(params(sort=sort), client=mg)
    request = backend._request_report_api.call_args.args[1]
    assert request.order_bys[0].desc
    assert request.order_bys[0].metric.metric_name == "sessions"
    assert request.order_bys[1].dimension.dimension_name == "date"


def test_limit_stops_ga4_pagination_at_requested_count():
    mg, backend = session()
    backend._request_report_api.return_value = (
        [["20260101", 1], ["20260102", 2]], 300000, ["date", "sessions"], [],
    )
    result = run_query(params(limit=2), client=mg)
    assert len(result.df) == 2
    assert backend._request_report_api.call_count == 1
    assert backend._request_report_api.call_args.args[1].limit == 2


def test_limit_stops_real_gsc_pagination():
    mg = Megaton.__new__(Megaton)
    api = Mock()
    api.searchanalytics.return_value.query.return_value.execute.return_value = {
        "rows": [{"keys": ["2026-01-01"], "clicks": 1, "impressions": 2, "ctr": .5, "position": 3}]
    }
    mg._retry = {}
    mg._gsc_service = GSCService(mg, client=api)
    mg.search = Megaton.Search(mg)
    assert len(run_query(params("gsc", limit=1), client=mg).df) == 1
    assert api.searchanalytics.return_value.query.call_count == 1


def test_gsc_last_page_requests_only_remaining_rows():
    api = Mock()
    row = {"keys": ["2026-01-01"], "clicks": 1, "impressions": 2, "ctr": .5, "position": 3}
    api.searchanalytics.return_value.query.return_value.execute.side_effect = [
        {"rows": [row, row]}, {"rows": [row]},
    ]
    service = GSCService(SimpleNamespace(_retry={}), client=api)
    frame = service.query("sc-domain:example.com", "2026-01-01", "2026-01-31",
                          ["date"], row_limit=2, max_rows=3)
    assert len(frame) == 3
    calls = api.searchanalytics.return_value.query.call_args_list
    assert [call.kwargs["body"]["rowLimit"] for call in calls] == [2, 1]
    assert [call.kwargs["body"]["startRow"] for call in calls] == [0, 2]


def test_search_appearance_passes_validation_and_real_gsc_execution():
    mg = Megaton.__new__(Megaton)
    api = Mock()
    api.searchanalytics.return_value.query.return_value.execute.return_value = {
        "rows": [{"keys": ["AMP_BLUE_LINK"], "clicks": 1, "impressions": 2, "ctr": .5, "position": 3}]
    }
    mg._retry = {}
    mg._gsc_service = GSCService(mg, client=api)
    mg.search = Megaton.Search(mg)
    appearance_filter = {"dimension": "searchAppearance", "operator": "equals", "expression": "AMP_BLUE_LINK"}
    request = params("gsc", dimensions=["searchAppearance"], filter_d=[appearance_filter])
    validate_query(request)
    result = run_query(request, client=mg)
    assert result.df.searchAppearance.tolist() == ["AMP_BLUE_LINK"]
    body = api.searchanalytics.return_value.query.call_args.kwargs["body"]
    assert body["dimensions"] == ["searchAppearance"]
    assert body["dimensionFilterGroups"][0]["filters"] == [appearance_filter]


def test_search_appearance_validate_only_cli(monkeypatch, capsys):
    monkeypatch.setattr("sys.stdin", io.StringIO(json.dumps(params("gsc", dimensions=["searchAppearance"]))))
    execute = Mock(side_effect=AssertionError("validate-only must not execute"))
    monkeypatch.setattr(cli, "run_query", execute)
    assert cli.main(["query", "--params", "-", "--validate-only"]) == 0
    assert json.loads(capsys.readouterr().out)["validated"] is True
    execute.assert_not_called()


@pytest.mark.parametrize("dimensions", [
    ["query", "searchAppearance"], ["searchAppearance", "date"],
    ["searchAppearance", "month"], ["searchAppearance", "searchAppearance"],
])
def test_search_appearance_cannot_be_combined(dimensions, monkeypatch, capsys):
    request = params("gsc", dimensions=dimensions)
    with pytest.raises(QueryValidationError, match="only grouping dimension"):
        validate_query(request)
    api = Mock()
    with pytest.raises(ValueError, match="only grouping dimension"):
        GSCService(None, client=api).query("sc-domain:example.com", "2026-01-01", "2026-01-31", dimensions)
    assert not api.mock_calls
    monkeypatch.setattr("sys.stdin", io.StringIO(json.dumps(request)))
    assert cli.main(["query", "--params", "-", "--validate-only"]) == 2
    assert json.loads(capsys.readouterr().out)["error_code"] == "INVALID_REQUEST"


def test_search_appearance_filter_allows_other_grouping_dimensions():
    appearance_filter = {"dimension": "searchAppearance", "operator": "equals", "expression": "AMP_BLUE_LINK"}
    request = params("gsc", dimensions=["query"], filter_d=[appearance_filter])
    assert validate_query(request)["dimensions"] == ["query"]
    api = Mock()
    api.searchanalytics.return_value.query.return_value.execute.return_value = {"rows": []}
    GSCService(None, client=api).query("sc-domain:example.com", "2026-01-01", "2026-01-31",
                                     ["query"], dimension_filter=[appearance_filter])
    body = api.searchanalytics.return_value.query.call_args.kwargs["body"]
    assert body["dimensions"] == ["query"]
    assert body["dimensionFilterGroups"][0]["filters"] == [appearance_filter]


@pytest.mark.parametrize("changes", [
    {"filter_d": ["country==JP"]}, {"filter_d": {"or": []}},
    {"filter_m": {"not": None}}, {"sort": [["sessions", "alias"]]},
    {"sort": "missing"}, {"sort": "--sessions"},
])
def test_validate_only_rejects_nonexecutable_ga4_shapes(changes):
    with pytest.raises(QueryValidationError):
        validate_query(params(**changes))


@pytest.mark.parametrize("changes", [
    {"dimensions": [["page", "url"]]}, {"dimensions": ["invalid"]},
    {"dimensions": ["date", "month"]},
    {"filter_d": [{"dimension": "page", "operator": "invalid", "expression": "x"}]},
    {"filter_d": [{"dimension": "page", "operator": "equals", "expression": []}]},
])
def test_validate_only_rejects_nonexecutable_gsc_shapes(changes):
    with pytest.raises(QueryValidationError):
        validate_query(params("gsc", **changes))


def test_ga4_filter_tree_reaches_real_formatter():
    mg, backend = session()
    result = run_query(params(filter_m={"or": ["sessions>1", "sessions<10"]}), client=mg)
    assert len(result.df) == 1
    assert len(backend._request_report_api.call_args.args[1].metric_filter.or_group.expressions) == 2


def test_path_collapse_reaggregates_weighted_metrics():
    mg = Mock()
    mg.search.run.return_value.df = pd.DataFrame({
        "page": ["https://a.example/x?a=1", "https://b.example/x?a=2"],
        "clicks": [2, 8], "impressions": [10, 40], "ctr": [.2, .2], "position": [2., 7.],
    })
    result = run_query(params("gsc", dimensions=["page"], page_to_path=True), client=mg)
    assert len(result.df) == 1
    row = result.df.iloc[0]
    assert (row["page"], row["clicks"], row["impressions"]) == ("/x", 10, 50)
    assert row["ctr"] == pytest.approx(.2)
    assert row["position"] == pytest.approx(6.)


def test_gsc_standalone_never_builds_ga_clients(monkeypatch):
    from megaton import start

    def auth(self, **kwargs):
        self.creds = object()
        self._build_ga_clients()

    monkeypatch.setattr(start.Megaton, "auth", auth)
    initialize = Mock(side_effect=AssertionError("GA4 discovery must not run"))
    monkeypatch.setattr("megaton.ga4.MegatonGA4", initialize)
    monkeypatch.setattr(start.GSCService, "list_sites", lambda self: ["sc-domain:example.com"])
    assert list_resources("gsc", credential="chosen.json") == ["sc-domain:example.com"]
    monkeypatch.setattr(start.GSCService, "query", lambda self, **kwargs: pd.DataFrame({"date": ["2026-01-01"], "clicks": [1]}))
    assert len(run_query(params("gsc"), credential="chosen.json").df) == 1
    initialize.assert_not_called()


def test_ga4_discovery_failure_is_not_success_with_empty_list(monkeypatch):
    from megaton import start

    failure = RuntimeError("discovery failed")
    mg = Mock(creds=object(), _ga_initialization_error=failure)
    monkeypatch.setattr(start, "Megaton", Mock(return_value=mg))
    with pytest.raises(RuntimeError) as caught:
        list_resources("ga4", credential="chosen.json")
    assert caught.value.__cause__ is failure


def test_cli_authentication_failure_is_exit_one(monkeypatch, capsys):
    monkeypatch.setattr("sys.stdin", io.StringIO(json.dumps(params())))
    monkeypatch.setattr(cli, "run_query", Mock(side_effect=QueryAuthenticationError("failed")))
    assert cli.main(["query", "--params", "-", "--credential", "chosen.json"]) == 1
    assert json.loads(capsys.readouterr().out)["error_code"] == "EXECUTION_FAILED"
