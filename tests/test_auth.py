import importlib
import json
import os
import sys
import types

import logging
import pytest


# ---------------------------------------------------------------------------
# Provide lightweight stand-ins for Google libraries when they are unavailable.
# This keeps the auth helpers importable inside a minimal test environment.
# ---------------------------------------------------------------------------

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


from megaton import auth


def test_is_service_account_detects_service_account_json():
    service_account_json = json.dumps({"type": "service_account", "project_id": "demo"})
    assert auth._is_service_account(service_account_json)
    assert not auth._is_service_account("not-json")
    assert not auth._is_service_account(json.dumps({"type": "user"}))


def test_get_credential_type_identifies_known_types():
    web_config = {
        "web": {
            "auth_uri": "https://example.com/auth",
            "token_uri": "https://example.com/token",
            "client_id": "abc123",
        }
    }
    installed_config = {
        "installed": {
            "auth_uri": "https://example.com/auth",
            "token_uri": "https://example.com/token",
            "client_id": "xyz789",
        }
    }

    assert auth.get_credential_type({"type": "service_account"}) == "service_account"
    assert auth.get_credential_type(web_config) == "web"
    assert auth.get_credential_type(installed_config) == "installed"
    assert auth.get_credential_type({"installed": {}}) is None


def test_get_credential_type_from_file(tmp_path):
    sa_file = tmp_path / "service.json"
    sa_file.write_text(json.dumps({"type": "service_account"}))

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

    assert auth.get_credential_type_from_file(sa_file) == "service_account"
    assert auth.get_credential_type_from_file(oauth_file) == "installed"


def test_get_json_files_from_dir_classifies_files(tmp_path):
    root = tmp_path / "creds"
    root.mkdir()
    sa_dir = root / "sa"
    sa_dir.mkdir()
    oauth_dir = root / "oauth"
    oauth_dir.mkdir()

    (sa_dir / "sa.json").write_text(json.dumps({"type": "service_account"}))
    (oauth_dir / "client.json").write_text(
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

    result = auth.get_json_files_from_dir(root)
    assert result["Service Account"]["sa.json"] == os.path.join(sa_dir, "sa.json")
    assert result["OAuth"]["client.json"] == os.path.join(oauth_dir, "client.json")


def test_get_credential_type_from_info_handles_variants():
    assert auth.get_credential_type_from_info({"type": "service_account"}) == "service_account"
    assert auth.get_credential_type_from_info({"installed": {}}) == "installed"
    assert auth.get_credential_type_from_info({"web": {}}) == "web"
    assert auth.get_credential_type_from_info({}) == "unknown"


def test_get_cache_path_uses_home_config_dir(tmp_path, monkeypatch):
    monkeypatch.setattr(auth.google_auth.os.path, "expanduser", lambda _: str(tmp_path))
    cache = auth.get_cache_path("client-secret.json")
    assert cache.endswith(".config/cache_client-secret.json")
    assert os.path.isdir(os.path.join(tmp_path, ".config"))

def test_save_credentials_writes_json(tmp_path, monkeypatch):
    cache_file = tmp_path / "cache.json"
    monkeypatch.setattr(auth.google_auth, "get_cache_path", lambda _: str(cache_file))

    class DummyCred:
        def __init__(self):
            self.data = {"token": "abc"}

        def to_json(self):
            return json.dumps(self.data)

    cred = DummyCred()
    returned = auth.save_credentials("client.json", cred)
    assert returned is cred
    assert cache_file.read_text() == json.dumps({"token": "abc"})


def test_load_credentials_returns_none_when_cache_missing(tmp_path, monkeypatch):
    cache_file = tmp_path / "missing.json"
    monkeypatch.setattr(auth.google_auth, "get_cache_path", lambda _: str(cache_file))
    assert auth.load_credentials("client.json", scopes=["scope"]) is None


def test_load_credentials_reads_from_cache(monkeypatch, tmp_path):
    cache_file = tmp_path / "cache.json"
    cache_file.write_text("{}")
    monkeypatch.setattr(auth.google_auth, "get_cache_path", lambda _: str(cache_file))

    class DummyCredentials:
        last_call = None

        @classmethod
        def from_authorized_user_file(cls, filename, scopes):
            cls.last_call = (filename, tuple(scopes))
            return cls()

    monkeypatch.setattr(auth.google_auth, "Credentials", DummyCredentials)
    creds = auth.load_credentials("client.json", scopes=["a", "b"])
    assert isinstance(creds, DummyCredentials)
    assert DummyCredentials.last_call == (str(cache_file), ("a", "b"))




def test_load_service_account_credentials_from_info_refreshes(monkeypatch):
    class DummyCred:
        def __init__(self, info):
            self.valid = False
            self.refreshed = False
            self.scopes = None
            self.info = info
            self.service_account_email = info.get('client_email')

        def refresh(self, request):
            self.valid = True
            self.refreshed = True

    class DummyFactory:
        last = None

        @classmethod
        def from_service_account_info(cls, info, scopes=None):
            cred = DummyCred(info)
            cred.scopes = tuple(scopes) if scopes is not None else None
            cls.last = cred
            return cred

    monkeypatch.setattr(auth.google_auth.service_account, 'Credentials', DummyFactory)
    monkeypatch.setattr(auth.google_auth.google.auth.transport.requests, 'Request', lambda: object())
    result = auth.load_service_account_credentials_from_info({'type': 'service_account', 'client_email': 'sa@example.com'}, ['scope-a'])
    assert result is DummyFactory.last
    assert result.valid
    assert result.refreshed
    assert result.scopes == ('scope-a',)
def test_load_service_account_credentials_from_info_refresh_failure(monkeypatch, caplog):
    class DummyCred:
        def __init__(self, info):
            self.valid = False
            self.service_account_email = info.get('client_email')

        def refresh(self, request):
            raise auth.google_auth.google.auth.exceptions.RefreshError('boom')

    class DummyFactory:
        @classmethod
        def from_service_account_info(cls, info, scopes=None):
            return DummyCred(info)

    monkeypatch.setattr(auth.google_auth.service_account, 'Credentials', DummyFactory)
    monkeypatch.setattr(auth.google_auth.google.auth.transport.requests, 'Request', lambda: object())
    with caplog.at_level(logging.ERROR):
        result = auth.load_service_account_credentials_from_info({'type': 'service_account', 'client_email': 'sa@example.com'}, ['scope'])
    assert result is None
    assert 'sa@example.com' in caplog.text
    assert '存在しない' in caplog.text


def test_load_service_account_credentials_from_file_refresh_failure(monkeypatch, tmp_path, caplog):
    sa_path = tmp_path / 'sa.json'
    sa_path.write_text('{}')

    class DummyCred:
        def __init__(self):
            self.valid = False
            self.service_account_email = 'sa@example.com'

        def refresh(self, request):
            raise auth.google_auth.google.auth.exceptions.RefreshError('boom')

    class DummyFactory:
        @classmethod
        def from_service_account_file(cls, path, scopes=None):
            return DummyCred()

    monkeypatch.setattr(auth.google_auth.service_account, 'Credentials', DummyFactory)
    monkeypatch.setattr(auth.google_auth.google.auth.transport.requests, 'Request', lambda: object())

    with caplog.at_level(logging.ERROR):
        result = auth.load_service_account_credentials_from_file(str(sa_path), ['scope'])
    assert result is None
    assert 'sa@example.com' in caplog.text
    assert '存在しない' in caplog.text

def test_delete_credentials_removes_file(tmp_path):
    cache_file = tmp_path / "cache.json"
    cache_file.write_text("data")
    assert cache_file.exists()
    auth.delete_credentials(str(cache_file))
    assert not cache_file.exists()
