"""Contracts for machine callers, including auth and failure boundaries."""

import io
import json
from unittest.mock import Mock

import pandas as pd
import pytest

from megaton import cli
from megaton.query import (
    QueryAuthenticationError,
    QueryResult,
    QueryValidationError,
    list_resources,
    run_query,
    validate_query,
)


def request(source="ga4"):
    result = {"schema_version": "1.0", "source": source}
    if source == "bigquery":
        return {**result, "project_id": "example-project", "sql": "SELECT 1", "location": "US"}
    return {
        **result, "property_id" if source == "ga4" else "site_url": "123" if source == "ga4" else "sc-domain:example.com",
        "date_range": {"start": "2026-01-01", "end": "2026-01-31"},
        "dimensions": ["date"], **({"metrics": ["sessions"]} if source == "ga4" else {}),
    }


@pytest.mark.parametrize("change", [
    {"schema_version": "2"}, {"source": "aa"}, {"source": []},
    {"limit": True}, {"limit": 0}, {"limit": 100001}, {"limit": "3"},
    {"dimensions": "date"}, {"metrics": []}, {"dimensions": [["date", ""]]},
    {"date_range": {"start": "20260101", "end": "2026-01-31"}},
    {"date_range": {"start": "2026-02-01", "end": "2026-01-31"}},
    {"date_range": {"start": "today", "end": "today"}},
    {"credential": "secret"}, {"filter_d": {}},
])
def test_invalid_input_before_execution(change):
    client = Mock()
    with pytest.raises(QueryValidationError):
        run_query({**request(), **change}, client=client)
    assert not client.mock_calls


def test_aliases_and_defaults_do_not_mutate_input():
    raw = request()
    raw["dimensions"] = [["date", "day"]]
    normalized = validate_query(raw)
    assert normalized["dimensions"] == [("date", "day")]
    assert "limit" not in raw
    assert normalized["limit"] == 1000


def test_ga4_explicit_session_and_result():
    client = Mock()
    client.report.run.return_value.df = pd.DataFrame({"sessions": [3]})
    result = run_query(request(), client=client)
    client.use_property.assert_called_once_with("123")
    client.report.set.dates.assert_called_once_with("2026-01-01", "2026-01-31")
    assert client.report.run.call_args.kwargs["show"] is False
    assert result.to_dict()["data"] == [{"sessions": 3}]


def test_gsc_filter_and_path_conversion_without_mutation():
    client = Mock()
    frame = pd.DataFrame({"page": ["https://example.com/a?q=x"], "clicks": [1]})
    client.search.run.return_value.df = frame
    params = {**request("gsc"), "dimensions": ["page"], "page_to_path": True,
              "filter_d": [{"dimension": "query", "operator": "equals", "expression": "test"}]}
    result = run_query(params, client=client)
    assert result.df.page.tolist() == ["/a"]
    assert frame.page.tolist() == ["https://example.com/a?q=x"]
    assert client.search.run.call_args.kwargs["filter_d"] == params["filter_d"]


def test_bq_location_and_no_optional_storage_dependency():
    client = Mock()
    client.query.return_value.result.return_value.to_dataframe.return_value = pd.DataFrame({"x": [1]})
    result = run_query(request("bigquery"), client=client)
    client.query.assert_called_once_with("SELECT 1", location="US")
    client.query.return_value.result.return_value.to_dataframe.assert_called_once_with(create_bqstorage_client=False)
    assert result.df.x.tolist() == [1]


@pytest.mark.parametrize("source", ["ga4", "gsc", "bigquery"])
def test_no_implicit_credentials(source):
    with pytest.raises(QueryValidationError):
        run_query(request(source))


def test_ambiguous_auth_rejected():
    with pytest.raises(QueryValidationError):
        run_query(request(), client=Mock(), credential="secret.json")


def test_credential_and_adc_are_mutually_exclusive():
    with pytest.raises(QueryValidationError):
        run_query(request("bigquery"), credential="chosen.json", adc=True)
    with pytest.raises(QueryValidationError):
        list_resources("bigquery", project_id="example", credential="chosen.json", adc=True)


def test_bq_discovery_does_not_ignore_oauth_cache_option():
    with pytest.raises(QueryValidationError):
        list_resources("bigquery", project_id="example", adc=True, cache_key="existing-cache")


def test_api_error_is_not_an_empty_result():
    client = Mock()
    client.report.run.side_effect = PermissionError("denied")
    with pytest.raises(PermissionError):
        run_query(request(), client=client)


def test_missing_result_is_failure():
    client = Mock()
    client.report.run.return_value = None
    with pytest.raises(RuntimeError):
        run_query(request(), client=client)


def test_empty_result_is_success():
    client = Mock()
    client.report.run.return_value.df = pd.DataFrame(columns=["sessions"])
    assert run_query(request(), client=client).to_dict()["row_count"] == 0


def test_output_dates_nulls_and_files(tmp_path):
    frame = pd.DataFrame({"date": [pd.Timestamp("2026-01-01")], "x": [float("nan")]})
    result = QueryResult(frame, request())
    result.save(tmp_path / "result.json")
    assert json.loads((tmp_path / "result.json").read_text())["data"][0]["x"] is None
    result.save(tmp_path / "result.csv")
    assert (tmp_path / "result.csv").read_text().startswith("date,x")


def test_cli_stdin_validate_without_auth(monkeypatch, capsys):
    monkeypatch.setattr("sys.stdin", io.StringIO(json.dumps(request())))
    assert cli.main(["query", "--params", "-", "--validate-only"]) == 0
    assert json.loads(capsys.readouterr().out)["validated"] is True


def test_cli_invalid_input_json(monkeypatch, capsys):
    monkeypatch.setattr("sys.stdin", io.StringIO("[]"))
    assert cli.main(["query", "--params", "-"]) == 2
    assert json.loads(capsys.readouterr().out)["error_code"] == "INVALID_REQUEST"


def test_cli_failure_does_not_expose_provider_message(monkeypatch, capsys):
    monkeypatch.setattr("sys.stdin", io.StringIO(json.dumps(request())))
    monkeypatch.setattr(cli, "run_query", Mock(side_effect=RuntimeError("sensitive provider response")))
    assert cli.main(["query", "--params", "-", "--credential", "secret.json"]) == 1
    output = capsys.readouterr().out
    assert "sensitive" not in output
    assert json.loads(output)["error_type"] == "RuntimeError"


def test_cli_captures_legacy_prints(monkeypatch, capsys):
    monkeypatch.setattr("sys.stdin", io.StringIO(json.dumps(request())))

    def execute(*args, **kwargs):
        print("legacy log")
        return QueryResult(pd.DataFrame({"x": [1]}), request())

    monkeypatch.setattr(cli, "run_query", execute)
    assert cli.main(["query", "--params", "-"]) == 0
    streams = capsys.readouterr()
    assert json.loads(streams.out)["row_count"] == 1
    assert "legacy log" in streams.err


def test_explicit_file_auth_uses_headless_session(monkeypatch):
    from megaton import start

    client = Mock()
    client.report.run.return_value.df = pd.DataFrame({"sessions": [1]})
    factory = Mock(return_value=client)
    monkeypatch.setattr(start, "Megaton", factory)
    run_query(request(), credential="chosen.json", cache_key="existing-cache")
    factory.assert_called_once_with("chosen.json", headless=True, cache_key="existing-cache")
    client.use_property.assert_called_once_with("123")


def test_auth_failure_stops_before_query(monkeypatch):
    from megaton import start

    client = Mock(creds=None)
    monkeypatch.setattr(start, "Megaton", Mock(return_value=client))
    with pytest.raises(QueryAuthenticationError):
        run_query(request(), credential="chosen.json")
    client.report.run.assert_not_called()


@pytest.mark.parametrize("source,method", [("ga4", "properties"), ("gsc", "sites")])
def test_discovery_uses_explicit_credential(monkeypatch, source, method):
    from megaton import start

    client = Mock()
    client._ga_initialization_error = None
    getattr(client, method).return_value = ["resource"]
    monkeypatch.setattr(start, "Megaton", Mock(return_value=client))
    assert list_resources(source, credential="chosen.json") == ["resource"]


def test_cli_file_output_is_a_reference(monkeypatch, capsys, tmp_path):
    monkeypatch.setattr("sys.stdin", io.StringIO(json.dumps(request())))
    monkeypatch.setattr(cli, "run_query", Mock(return_value=QueryResult(pd.DataFrame({"x": [1]}), request())))
    target = tmp_path / "result.csv"
    assert cli.main(["query", "--params", "-", "--output", str(target)]) == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload == {"status": "ok", "output": str(target), "row_count": 1}
    assert target.read_text() == "x\n1\n"


def test_invalid_output_extension_stops_before_execution(monkeypatch, tmp_path, capsys):
    monkeypatch.setattr("sys.stdin", io.StringIO(json.dumps(request())))
    execute = Mock()
    monkeypatch.setattr(cli, "run_query", execute)
    assert cli.main(["query", "--params", "-", "--output", str(tmp_path / "bad.txt")]) == 2
    execute.assert_not_called()
    assert json.loads(capsys.readouterr().out)["error_code"] == "INVALID_REQUEST"
