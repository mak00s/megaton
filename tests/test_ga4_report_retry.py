from types import SimpleNamespace

from google.api_core.exceptions import ServiceUnavailable

from megaton import ga4


class _FakeDataClient:
    def __init__(self, responses):
        self._responses = list(responses)
        self.calls = 0

    def run_report(self, request):
        self.calls += 1
        result = self._responses.pop(0)
        if isinstance(result, Exception):
            raise result
        return result


class _DummyParent:
    def __init__(self, data_client):
        self.data_client = data_client


def test_request_report_api_retries_service_unavailable_then_succeeds(monkeypatch):
    client = _FakeDataClient([
        ServiceUnavailable("temporary"),
        SimpleNamespace(row_count=7),
    ])
    report = ga4.MegatonGA4.Report(_DummyParent(client))
    report._parse_response = lambda response: ([["ok"]], ["date"], ["TYPE_INTEGER"]) if response else ([], [], [])

    waits = []
    monkeypatch.setattr("megaton.ga4.time.sleep", waits.append)

    data, total_rows, headers, types = report._request_report_api(
        0,
        SimpleNamespace(),
        max_retries=2,
        backoff_factor=1.5,
    )

    assert client.calls == 2
    assert waits == [1.5]
    assert total_rows == 7
    assert data == [["ok"]]
    assert headers == ["date"]
    assert types == ["TYPE_INTEGER"]


def test_request_report_api_returns_empty_after_retry_exhaustion(monkeypatch):
    client = _FakeDataClient([
        ServiceUnavailable("temporary-1"),
        ServiceUnavailable("temporary-2"),
        ServiceUnavailable("temporary-3"),
    ])
    report = ga4.MegatonGA4.Report(_DummyParent(client))

    waits = []
    monkeypatch.setattr("megaton.ga4.time.sleep", waits.append)

    data, total_rows, headers, types = report._request_report_api(
        0,
        SimpleNamespace(),
        max_retries=3,
        backoff_factor=2.0,
    )

    assert client.calls == 3
    assert waits == [2.0, 4.0]
    assert total_rows == 0
    assert data == []
    assert headers == []
    assert types == []
