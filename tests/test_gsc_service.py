import pytest
from googleapiclient.errors import HttpError

from megaton.services.gsc_service import GSCService


class _FakeRequest:
    def __init__(self, parent, start_row):
        self.parent = parent
        self.start_row = start_row

    def execute(self):
        return self.parent.execute(self.start_row)


class _FakeSearchAnalytics:
    def __init__(self, responses, errors=None):
        self.responses = responses
        self.errors = {k: list(v) for k, v in (errors or {}).items()}
        self.calls = []

    def query(self, siteUrl, body):
        self.calls.append((siteUrl, body))
        start_row = body.get("startRow", 0)
        return _FakeRequest(self, start_row)

    def execute(self, start_row):
        if start_row in self.errors and self.errors[start_row]:
            raise self.errors[start_row].pop(0)
        return {"rows": self.responses.get(start_row, [])}


class _FakeClient:
    def __init__(self, responses, errors=None):
        self.analytics = _FakeSearchAnalytics(responses, errors=errors)

    def searchanalytics(self):
        return self.analytics


def _http_error(status=500):
    resp = type("Resp", (), {"status": status, "reason": "error"})()
    return HttpError(resp, b"{}")


def test_query_paging_clean_and_aggregate():
    responses = {
        0: [
            {
                "keys": ["q1", "https://example.com/Page?x=1"],
                "clicks": 1,
                "impressions": 10,
                "position": 2.0,
            },
            {
                "keys": ["q1", "https://example.com/Page?x=1"],
                "clicks": 2,
                "impressions": 20,
                "position": 4.0,
            },
            {
                "keys": ["q3"],
                "clicks": 999,
                "impressions": 999,
                "position": 9.0,
            },
        ],
        3: [
            {
                "keys": ["q2", "https://example.com/Other#frag"],
                "clicks": 3,
                "impressions": 30,
                "position": 1.0,
            },
        ],
    }
    client = _FakeClient(responses)
    service = GSCService(app=None, client=client)

    df = service.query(
        site_url="https://example.com",
        start_date="2024-01-01",
        end_date="2024-01-31",
        dimensions=["query", "page"],
        row_limit=3,
        clean=True,
        aggregate=True,
    )

    assert len(client.analytics.calls) == 2
    assert set(df["query"]) == {"q1", "q2"}

    row_q1 = df[df["query"] == "q1"].iloc[0]
    assert row_q1["page"] == "https://example.com/page"
    assert row_q1["clicks"] == 3
    assert row_q1["impressions"] == 30
    assert row_q1["position"] == pytest.approx((2 * 10 + 4 * 20) / 30, rel=1e-6)


def test_query_retries_on_http_error(monkeypatch):
    responses = {
        0: [
            {
                "keys": ["q1", "https://example.com"],
                "clicks": 1,
                "impressions": 10,
                "position": 2.0,
            }
        ]
    }
    errors = {0: [_http_error()]}
    client = _FakeClient(responses, errors=errors)
    service = GSCService(app=None, client=client)

    monkeypatch.setattr("megaton.services.gsc_service.time.sleep", lambda _: None)

    df = service.query(
        site_url="https://example.com",
        start_date="2024-01-01",
        end_date="2024-01-31",
        dimensions=["query", "page"],
        row_limit=25000,
        max_retries=2,
    )

    assert len(client.analytics.calls) == 2
    assert not df.empty
