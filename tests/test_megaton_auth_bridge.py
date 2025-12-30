import importlib
import sys
import types


def ensure_module(name: str):
    parts = name.split('.')
    module_name = ''
    parent = None
    for part in parts:
        module_name = f"{module_name}.{part}" if module_name else part
        module = sys.modules.get(module_name)
        if module is None:
            module = types.ModuleType(module_name)
            sys.modules[module_name] = module
            if parent is not None:
                setattr(parent, part, module)
        parent = module
    return module


def get_module(name: str):
    try:
        return importlib.import_module(name)
    except Exception:
        return ensure_module(name)


data_mod = get_module("google.analytics.data")
if not hasattr(data_mod, "BetaAnalyticsDataClient"):
    class _StubDataClient:
        pass

    data_mod.BetaAnalyticsDataClient = _StubDataClient

admin_mod = get_module("google.analytics.admin")
if not hasattr(admin_mod, "AnalyticsAdminServiceClient"):
    class _StubAdminClient:
        pass

    admin_mod.AnalyticsAdminServiceClient = _StubAdminClient


from megaton import start


def test_megaton_auth_bridge_service_account(monkeypatch):
    calls = {}

    def fake_resolve(credential, in_colab=False, **kwargs):
        calls["resolve"] = {"credential": credential, "in_colab": in_colab}
        return start.auth_provider.CredentialSource(
            raw=credential,
            origin="explicit",
            kind="inline",
            info={"type": "service_account", "client_email": "demo@example.com"},
            credential_type="service_account",
        )

    dummy_creds = object()

    def fake_load(info, scopes):
        calls["load"] = {"info": info, "scopes": tuple(scopes)}
        return dummy_creds

    monkeypatch.setattr(start.auth_provider, "resolve_credential_source", fake_resolve)
    monkeypatch.setattr(start.auth_google, "load_service_account_credentials_from_info", fake_load)

    mg = start.Megaton.__new__(start.Megaton)
    mg.required_scopes = ["scope-a"]
    mg.creds = None
    mg.headless = True
    mg._build_ga_clients = lambda: None
    mg._reset_pending_oauth = lambda: None
    mg._notify_invalid_service_account = lambda email=None: None

    mg.auth(credential="dummy")

    assert calls["resolve"]["credential"] == "dummy"
    assert "load" in calls
    assert calls["load"]["info"]["type"] == "service_account"
    assert calls["load"]["scopes"] == ("scope-a",)
    assert mg.creds is dummy_creds
