"""Keyless authentication contracts, using only synthetic configs and mocked clients."""
import json
from types import SimpleNamespace
from urllib.parse import quote

import google.auth
import pytest

from megaton.auth import google_auth, hints, provider
from megaton.auth.store import CredentialStore

EMAIL = "actions@demo.iam.gserviceaccount.com"
SCOPES = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]


@pytest.fixture
def external_info():
    return {
        "type": "external_account",
        "audience": "//iam.googleapis.com/projects/123/locations/global/workloadIdentityPools/github/providers/actions",
        "subject_token_type": "urn:ietf:params:oauth:token-type:jwt",
        "token_url": "https://sts.googleapis.com/v1/token",
        "credential_source": {"file": "/unused/oidc-subject"},
        "service_account_impersonation_url": (
            "https://iamcredentials.googleapis.com/v1/projects/-/serviceAccounts/"
            + EMAIL + ":generateAccessToken"
        ),
    }


@pytest.fixture
def external_file(tmp_path, external_info):
    path = tmp_path / "gha-creds.json"
    path.write_text(json.dumps(external_info))
    return path


@pytest.fixture
def loaded(monkeypatch):
    credentials = object()
    calls = []
    def load(path, *, scopes):
        calls.append((str(path), scopes))
        return credentials, "demo"
    monkeypatch.setattr(google.auth, "load_credentials_from_file", load)
    return credentials, calls


def test_external_classification_and_email(external_file, external_info):
    assert google_auth.get_credential_type(external_info) == "external_account"
    assert google_auth.get_credential_type_from_info(external_info) == "external_account"
    assert google_auth.get_credential_type_from_file(str(external_file)) == "external_account"
    assert provider.extract_email_from_file(str(external_file)) == EMAIL
    assert provider.extract_email_from_info(external_info) == EMAIL
    external_info["service_account_impersonation_url"] = external_info["service_account_impersonation_url"].replace(EMAIL, quote(EMAIL, safe=""))
    assert provider.extract_email_from_info(external_info) == EMAIL


@pytest.mark.parametrize("url", [None, "", 42, "not a URL", "https://[invalid",
    "https://example.com/v1/projects/-/serviceAccounts/a@b:generateAccessToken",
    "https://iamcredentials.googleapis.com/v1/projects/-/serviceAccounts/:generateAccessToken",
    "https://iamcredentials.googleapis.com/v1/projects/-/serviceAccounts/a%0Ab@c:generateAccessToken"])
def test_unknown_email_is_safe(external_info, url):
    external_info["service_account_impersonation_url"] = url
    assert provider.extract_email_from_info(external_info) is None


def test_service_account_email_unchanged():
    assert provider.extract_email_from_info({"type": "service_account", "client_email": EMAIL}) == EMAIL
    assert provider.extract_email_from_info({}) is None


def test_adc_env_priority(monkeypatch, external_file):
    monkeypatch.delenv("MEGATON_CREDS_JSON", raising=False)
    monkeypatch.setenv("GOOGLE_APPLICATION_CREDENTIALS", str(external_file))
    source = provider.resolve_credential_source(None, in_colab=True)
    assert (source.origin, source.kind, source.credential_type) == ("adc_env", "file", "external_account")
    monkeypatch.setenv("MEGATON_CREDS_JSON", json.dumps({"type": "service_account"}))
    assert provider.resolve_credential_source(None).credential_type == "service_account"
    assert provider.resolve_credential_source(str(external_file)).credential_type == "external_account"


def test_file_loader_scopes(external_file, loaded):
    creds, calls = loaded
    assert google_auth.load_service_account_credentials_from_file(str(external_file), SCOPES) is creds
    assert calls == [(str(external_file), SCOPES)]


def test_inline_loader_scopes(external_info, monkeypatch):
    creds = object()
    def load(info, *, scopes):
        assert info == external_info and scopes == SCOPES
        return creds, None
    monkeypatch.setattr(google.auth, "load_credentials_from_dict", load)
    assert google_auth.load_service_account_credentials_from_info(external_info, SCOPES) is creds


def test_real_google_loader_constructs_external_credentials(external_file, monkeypatch):
    from google.auth import identity_pool
    # Avoid discovery/refresh entirely, while exercising the installed parser and scoping.
    monkeypatch.setattr(identity_pool.Credentials, "get_project_id", lambda *a, **k: None)
    creds = google_auth.load_service_account_credentials_from_file(str(external_file), SCOPES)
    assert isinstance(creds, identity_pool.Credentials)
    assert creds.scopes == SCOPES
    assert creds.service_account_email == EMAIL


def test_loader_failure_propagates(external_file, monkeypatch):
    def fail(*a, **k):
        raise google.auth.exceptions.DefaultCredentialsError("invalid federation configuration")
    monkeypatch.setattr(google.auth, "load_credentials_from_file", fail)
    with pytest.raises(google.auth.exceptions.DefaultCredentialsError):
        google_auth.load_service_account_credentials_from_file(str(external_file), SCOPES)


def test_store_external_file_alias_and_scopes(tmp_path, external_info, loaded):
    store = CredentialStore(tmp_path / "store")
    store.init()
    path = store.sa_dir / "actions.json"
    path.write_text(json.dumps(external_info))
    [entry] = store.service_accounts()
    assert entry.client_email == EMAIL
    assert store.service_account_path("actions") == path
    assert store.service_account_credentials("actions", "sheets,drive") is loaded[0]
    assert loaded[1] == [(str(path), SCOPES)]


def test_no_key_import_hint_for_ephemeral_config(external_file, tmp_path):
    store = CredentialStore(tmp_path / "empty")
    assert hints.store_hint(external_file, store=store) is None
    store.init()
    (store.sa_dir / "actions.json").write_text(external_file.read_text())
    assert hints.store_hint(external_file, store=store) is None
    (store.sa_dir / "legacy.json").write_text(json.dumps({"type": "service_account", "client_email": EMAIL}))
    assert hints.store_hint(external_file, store=store) is None


@pytest.mark.parametrize("custom_scopes", [None, SCOPES])
def test_gspread_receives_external_credentials(external_file, loaded, monkeypatch, custom_scopes):
    import gspread
    from megaton import gsheet_lowlevel
    result = object()
    def authorize(creds):
        assert creds is loaded[0]
        return SimpleNamespace(open_by_key=lambda key: result if key == "sheet-id" else None)
    monkeypatch.setattr(gspread, "authorize", authorize)
    assert gsheet_lowlevel.open_spreadsheet(spreadsheet_id="sheet-id", credentials_path=external_file,
                                          scopes=custom_scopes) is result
    assert loaded[1] == [(str(external_file), list(custom_scopes or gsheet_lowlevel.DEFAULT_SHEETS_SCOPES))]


def test_gspread_key_file_route_unchanged(tmp_path, monkeypatch):
    import gspread
    from google.oauth2 import service_account
    from megaton import gsheet_lowlevel
    path = tmp_path / "key.json"
    path.write_text(json.dumps({"type": "service_account"}))
    creds = object()
    def load(filename, *, scopes):
        assert filename == str(path) and scopes == SCOPES
        return creds
    monkeypatch.setattr(service_account.Credentials, "from_service_account_file", load)
    monkeypatch.setattr(gspread, "authorize", lambda value: SimpleNamespace(open_by_key=lambda key: value))
    assert gsheet_lowlevel.open_spreadsheet(spreadsheet_id="id", credentials_path=path, scopes=SCOPES) is creds


def test_megaton_external_adc_passes_scopes_to_ga(external_file, loaded, monkeypatch):
    from megaton import start
    monkeypatch.delenv("MEGATON_CREDS_JSON", raising=False)
    monkeypatch.setenv("GOOGLE_APPLICATION_CREDENTIALS", str(external_file))
    monkeypatch.setattr(start, "_ensure_deps", lambda **kw: None)
    passed = []
    monkeypatch.setattr(start.Megaton, "_build_ga_clients", lambda self: passed.append(self.creds))
    mg = start.Megaton(headless=True, scopes=["sheets", "drive"])
    assert mg.creds is loaded[0] and passed == [loaded[0]]
    assert loaded[1] == [(str(external_file), SCOPES)]
    assert mg.json == str(external_file)


def test_bigquery_passes_external_credentials(external_file, loaded, monkeypatch):
    from google.cloud import bigquery
    from megaton.query import _bq_client
    def client(*, project, credentials):
        assert project == "demo" and credentials is loaded[0]
        return credentials
    monkeypatch.setattr(bigquery, "Client", client)
    assert _bq_client("demo", str(external_file), False) is loaded[0]
    assert loaded[1] == [(str(external_file), ["https://www.googleapis.com/auth/cloud-platform"])]


def test_docs_drive_receive_same_external_credentials(external_file, loaded, monkeypatch):
    from googleapiclient import discovery
    calls = []
    def build(service, version, *, credentials):
        calls.append((service, version, credentials))
        return SimpleNamespace(service=service)
    monkeypatch.setattr(discovery, "build", build)
    scopes = ["https://www.googleapis.com/auth/documents.readonly", SCOPES[1]]
    creds = google_auth.load_service_account_credentials_from_file(str(external_file), scopes)
    discovery.build("docs", "v1", credentials=creds)
    discovery.build("drive", "v3", credentials=creds)
    assert calls == [("docs", "v1", loaded[0]), ("drive", "v3", loaded[0])]
    assert loaded[1] == [(str(external_file), scopes)]


@pytest.fixture
def real_external_credentials(external_file, monkeypatch):
    from google.auth import identity_pool
    monkeypatch.setattr(identity_pool.Credentials, "get_project_id", lambda *a, **k: None)
    scopes = SCOPES + ["https://www.googleapis.com/auth/analytics.readonly",
                      "https://www.googleapis.com/auth/webmasters.readonly"]
    return google_auth.load_service_account_credentials_from_file(str(external_file), scopes)


def test_ga4_accepts_external_credentials(real_external_credentials, monkeypatch):
    from megaton import ga4
    received = []
    def build(*, credentials):
        received.append(credentials)
        return object()
    monkeypatch.setattr(ga4, "BetaAnalyticsDataClient", build)
    monkeypatch.setattr(ga4, "AnalyticsAdminServiceClient", build)
    monkeypatch.setattr(ga4.MegatonGA4, "_update", lambda self: True)
    client = ga4.MegatonGA4(real_external_credentials)
    assert client.credentials is real_external_credentials
    assert received == [real_external_credentials, real_external_credentials]


def test_notebook_gspread_accepts_external_credentials(real_external_credentials, monkeypatch):
    import gspread
    from megaton import gsheet
    received = []
    def authorize(credentials):
        received.append(credentials)
        return SimpleNamespace(http_client=SimpleNamespace())
    monkeypatch.setattr(gspread, "authorize", authorize)
    client = gsheet.MegatonGS(real_external_credentials)
    assert client.credentials is real_external_credentials
    assert received == [real_external_credentials]


def test_search_console_accepts_external_credentials(real_external_credentials, monkeypatch):
    from megaton import searchconsole
    received = []
    def build(service, version, *, credentials):
        received.append(credentials)
        return object()
    monkeypatch.setattr(searchconsole, "build", build)
    client = searchconsole.MegatonSC(real_external_credentials)
    assert client.credentials is real_external_credentials
    assert received == [real_external_credentials]
