import importlib
import json
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


credentials_mod = get_module('google.oauth2.credentials')
if not hasattr(credentials_mod, 'Credentials'):
    class _StubCredentials:
        pass

    credentials_mod.Credentials = _StubCredentials

service_account_mod = get_module('google.oauth2.service_account')
if not hasattr(service_account_mod, 'Credentials'):
    class _StubServiceAccount:
        pass

    service_account_mod.Credentials = _StubServiceAccount

flow_mod = get_module('google_auth_oauthlib.flow')
if not hasattr(flow_mod, 'InstalledAppFlow'):
    class _StubFlow:
        pass

    flow_mod.InstalledAppFlow = _StubFlow

transport_mod = get_module('google.auth.transport.requests')
if not hasattr(transport_mod, 'Request'):
    transport_mod.Request = lambda: object()

exceptions_mod = get_module('google.auth.exceptions')
if not hasattr(exceptions_mod, 'RefreshError'):
    class _StubRefreshError(Exception):
        pass

    exceptions_mod.RefreshError = _StubRefreshError


from megaton.auth import provider


def test_resolve_credential_source_uses_env(monkeypatch):
    sa_json = json.dumps({"type": "service_account", "project_id": "demo"})
    monkeypatch.setenv("MEGATON_CREDS_JSON", sa_json)

    source = provider.resolve_credential_source(None)
    assert source.origin == "env"
    assert source.kind == "inline"
    assert source.credential_type == "service_account"
    assert source.info["project_id"] == "demo"


def test_resolve_credential_source_accepts_dict():
    info = {"type": "service_account", "project_id": "demo"}
    source = provider.resolve_credential_source(info)
    assert source.kind == "inline"
    assert source.credential_type == "service_account"
    assert source.info == info


def test_resolve_credential_source_accepts_json_string():
    info = {"type": "service_account", "project_id": "demo"}
    source = provider.resolve_credential_source(json.dumps(info))
    assert source.kind == "inline"
    assert source.credential_type == "service_account"
    assert source.info["project_id"] == "demo"


def test_resolve_credential_source_accepts_file_path(tmp_path):
    oauth_file = tmp_path / "oauth.json"
    oauth_file.write_text(
        json.dumps(
            {
                "installed": {
                    "auth_uri": "a",
                    "token_uri": "b",
                    "client_id": "c",
                }
            }
        )
    )
    source = provider.resolve_credential_source(str(oauth_file))
    assert source.kind == "file"
    assert source.credential_type == "installed"
    assert source.info["installed"]["client_id"] == "c"


def test_resolve_credential_source_accepts_directory(tmp_path):
    source = provider.resolve_credential_source(str(tmp_path))
    assert source.kind == "directory"
