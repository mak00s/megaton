from types import SimpleNamespace
from unittest.mock import Mock

import pandas as pd
import pytest

from megaton.ga4 import MegatonGA4
from megaton.query import QueryCompleteness, run_query
from megaton.query_metadata import COMPLETENESS_ATTR
from megaton.services.gsc_service import GSCService


@pytest.mark.parametrize("total,status", [(2, "complete"), (3, "truncated")])
def test_ga4_exact_cap_uses_total_not_frame_length(monkeypatch, total, status):
    report = MegatonGA4.Report(SimpleNamespace(property=SimpleNamespace(id="123")))
    monkeypatch.setattr(report, "_format_request", lambda **kw: SimpleNamespace())
    monkeypatch.setattr(report, "_request_report_api", lambda *a, **kw:
                        ([["a", 1], ["b", 2]], total, ["country", "sessions"], ["category", "TYPE_INTEGER"]))
    frame = report.run(dimensions=["country"], metrics=["sessions"], limit=2, max_rows=2)
    evidence = QueryCompleteness.from_frame(frame)
    assert evidence.status == status
    assert evidence.fetched_rows == 2
    assert evidence.total_rows == total
    assert evidence.limit_reached is True


@pytest.mark.parametrize("count,cap,status", [(1, 2, "complete"), (2, 2, "unknown"), (0, 2, "complete")])
def test_gsc_records_raw_count_before_clean_aggregation(count, cap, status):
    rows = [{"keys": ["https://example.com/a?x=" + str(i)], "clicks": 1, "impressions": 2}
            for i in range(count)]
    analytics = SimpleNamespace(query=lambda **kw: SimpleNamespace(execute=lambda: {"rows": rows}))
    service = GSCService(None, client=SimpleNamespace(searchanalytics=lambda: analytics))
    frame = service.query(site_url="sc-domain:example.com", start_date="2026-01-01",
                          end_date="2026-01-31", dimensions=["page"], clean=True,
                          row_limit=cap, max_rows=cap)
    evidence = QueryCompleteness.from_frame(frame)
    assert evidence.status == status
    assert evidence.fetched_rows == count
    assert len(frame) <= 1
    assert evidence.total_rows is None


def test_gsc_discarded_malformed_rows_are_not_complete():
    analytics = SimpleNamespace(query=lambda **kw: SimpleNamespace(execute=lambda: {"rows": [
        {"keys": [], "clicks": 1},
    ]}))
    service = GSCService(None, client=SimpleNamespace(searchanalytics=lambda: analytics))
    frame = service.query(site_url="sc-domain:example.com", start_date="2026-01-01",
                          end_date="2026-01-31", dimensions=["page"], max_rows=5)
    evidence = QueryCompleteness.from_frame(frame)
    assert evidence.status == "unknown"
    assert evidence.discarded_rows == 1


def test_query_json_keeps_evidence_through_path_reaggregation():
    client = Mock()
    frame = pd.DataFrame({"page": ["https://example.com/a?x=1", "https://example.com/a?x=2"],
                          "clicks": [1, 2], "impressions": [2, 4], "position": [1., 2.]})
    frame.attrs[COMPLETENESS_ATTR] = QueryCompleteness(2, 2).to_dict()
    client.search.run.return_value.df = frame
    result = run_query({"schema_version": "1.0", "source": "gsc", "site_url": "sc-domain:example.com",
                        "date_range": {"start": "2026-01-01", "end": "2026-01-31"},
                        "dimensions": ["page"], "limit": 2, "page_to_path": True}, client=client)
    assert len(result.df) == 1
    assert result.completeness.fetched_rows == 2
    assert result.completeness.status == "unknown"
    assert result.to_dict()["completeness"]["limit_reached"] is True
    assert result.df.attrs[COMPLETENESS_ATTR] == result.completeness.to_dict()


def test_missing_or_invalid_metadata_does_not_infer_completeness_from_short_frame():
    frame = pd.DataFrame({"x": [1]})
    assert QueryCompleteness.from_frame(frame, row_limit=10).status == "unknown"
    frame.attrs[COMPLETENESS_ATTR] = {"status": "complete"}
    assert QueryCompleteness.from_frame(frame, row_limit=10).fetched_rows is None


@pytest.mark.parametrize("values", [
    {"fetched_rows": 1, "total_rows": 2, "status": "complete"},
    {"fetched_rows": 2, "total_rows": 2, "status": "truncated"},
    {"fetched_rows": 1, "discarded_rows": 1, "status": "complete"},
])
def test_contradictory_metadata_is_not_trusted(values):
    frame = pd.DataFrame({"x": [1]})
    frame.attrs[COMPLETENESS_ATTR] = {"row_limit": 10, "total_rows": None, "discarded_rows": 0, **values}
    assert QueryCompleteness.from_frame(frame).status == "unknown"


def test_cli_csv_output_retains_completeness(monkeypatch, tmp_path, capsys):
    import io
    import json
    from megaton import cli
    from megaton.query import QueryResult

    request = {"schema_version": "1.0", "source": "ga4", "property_id": "123",
               "date_range": {"start": "2026-01-01", "end": "2026-01-31"},
               "dimensions": ["date"], "metrics": ["sessions"]}
    evidence = QueryCompleteness(1, 1, total_rows=2, status="truncated")
    value = QueryResult(pd.DataFrame({"sessions": [1]}), request, evidence)
    monkeypatch.setattr("sys.stdin", io.StringIO(json.dumps(request)))
    monkeypatch.setattr(cli, "run_query", lambda *a, **k: value)
    assert cli.main(["query", "--params", "-", "--credential", "unused",
                     "--output", str(tmp_path / "result.csv")]) == 0
    assert json.loads(capsys.readouterr().out)["completeness"] == evidence.to_dict()
