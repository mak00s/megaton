from megaton.start import Megaton


def test_sc_sites_calls_service(monkeypatch):
    app = Megaton(None, headless=True)
    called = {}

    def fake_fetch_sites(*args, **kwargs):
        called["args"] = args
        called["kwargs"] = kwargs
        return "ok"

    monkeypatch.setattr(app._gsc_service, "fetch_sites", fake_fetch_sites)

    result = app.sc.sites(
        [{"clinic": "A", "url": "https://example.com"}],
        "A",
        "2024-01-01",
        "2024-01-31",
        ["query"],
        country="jpn",
    )

    assert result == "ok"
    assert called["args"][0] == [{"clinic": "A", "url": "https://example.com"}]
    assert called["args"][1] == "A"
