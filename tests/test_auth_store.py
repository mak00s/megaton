"""Credential store: identity lookup, scope checks, refresh persistence, CLI."""

import json
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest

from megaton import cli
from megaton.auth import store as store_mod
from megaton.auth.store import (
    CredentialStore,
    CredentialStoreError,
    USERINFO_SCOPE,
    expand_scopes,
    scopes_satisfied,
    short_scope,
)

DOCS = "https://www.googleapis.com/auth/documents"
DRIVE = "https://www.googleapis.com/auth/drive"
SHEETS = "https://www.googleapis.com/auth/spreadsheets"


def future():
    return (datetime.now(timezone.utc) + timedelta(hours=1)).strftime("%Y-%m-%dT%H:%M:%SZ")


def token(email="sim@example.com", scopes=(DOCS, USERINFO_SCOPE), expiry=None):
    return {
        "token": "access-old", "refresh_token": "refresh-1",
        "token_uri": "https://oauth2.googleapis.com/token",
        "client_id": "cid.apps.googleusercontent.com", "client_secret": "secret",
        "scopes": list(scopes), "account": email, "expiry": expiry or future(),
    }


def sa_key(name="sheets-api", project="proj"):
    return {
        "type": "service_account", "project_id": project,
        "client_email": f"{name}@{project}.iam.gserviceaccount.com",
        "private_key_id": "k", "private_key": "-----BEGIN PRIVATE KEY-----\nx\n-----END PRIVATE KEY-----\n",
        "client_id": "1", "token_uri": "https://oauth2.googleapis.com/token",
    }


@pytest.fixture
def home(tmp_path, monkeypatch):
    monkeypatch.setenv(store_mod.HOME_ENV, str(tmp_path / "store"))
    st = CredentialStore()
    st.init()
    return st


def write(path, payload):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload), encoding="utf-8")


# ---- scopes ---------------------------------------------------------------

def test_expand_scopes_accepts_aliases_comma_lists_and_urls():
    assert expand_scopes("docs, drive") == [DOCS, DRIVE]
    assert expand_scopes(["sheets", SHEETS, "sheets"]) == [SHEETS]
    assert expand_scopes(None) == []
    with pytest.raises(CredentialStoreError):
        expand_scopes("nope")


def test_scopes_satisfied_applies_implied_grants():
    assert scopes_satisfied([DRIVE], [SHEETS, DOCS]) == []
    assert scopes_satisfied(["https://mail.google.com/"], expand_scopes("gmail.readonly,gmail.send")) == []
    assert scopes_satisfied([DOCS], [DRIVE]) == [DRIVE]
    # GA4 Data API accepts analytics / analytics.readonly only; analytics.edit must not imply readonly.
    analytics_ro = "https://www.googleapis.com/auth/analytics.readonly"
    assert scopes_satisfied(["https://www.googleapis.com/auth/analytics.edit"], [analytics_ro]) == [analytics_ro]
    assert short_scope(DOCS) == "docs"


# ---- users ----------------------------------------------------------------

def test_resolve_email_by_alias_email_and_unique_local_part(home):
    write(home.user_token_path("sim@example.com"), token())
    home.registry_path.write_text('[users]\nme = "sim@example.com"\n', encoding="utf-8")
    assert home.resolve_email("me") == "sim@example.com"
    assert home.resolve_email("other@example.com") == "other@example.com"
    assert home.resolve_email("sim") == "sim@example.com"
    with pytest.raises(CredentialStoreError) as exc:
        home.resolve_email("ghost")
    assert "megaton auth login" in exc.value.hint


def test_resolve_email_rejects_ambiguous_local_part(home):
    write(home.user_token_path("sim@a.com"), token("sim@a.com"))
    write(home.user_token_path("sim@b.com"), token("sim@b.com"))
    with pytest.raises(CredentialStoreError, match="several"):
        home.resolve_email("sim")


def test_user_credentials_missing_token_hints_login(home):
    with pytest.raises(CredentialStoreError) as exc:
        home.user_credentials("sim@example.com")
    assert exc.value.hint == "megaton auth login --account sim@example.com --scopes docs,drive"


def test_user_credentials_checks_scopes_without_expanding_grant(home):
    write(home.user_token_path("sim@example.com"), token())
    with pytest.raises(CredentialStoreError) as exc:
        home.user_credentials("sim", scopes="docs,gmail.readonly")
    assert "gmail.readonly" in str(exc.value)
    assert exc.value.hint == "megaton auth login --account sim@example.com --scopes docs,gmail.readonly"
    creds = home.user_credentials("sim", scopes=["docs"])
    assert creds.token == "access-old"
    assert creds.valid


def test_user_credentials_refreshes_and_persists_expired_token(home, monkeypatch):
    write(home.user_token_path("sim@example.com"), token(expiry="2020-01-01T00:00:00Z"))

    def fake_refresh(self, creds, email):
        creds.token = "access-new"
        creds.expiry = datetime.now(timezone.utc).replace(tzinfo=None) + timedelta(hours=1)

    monkeypatch.setattr(CredentialStore, "_refresh", fake_refresh)
    creds = home.user_credentials("sim")
    assert creds.token == "access-new"
    saved = json.loads(home.user_token_path("sim@example.com").read_text())
    assert saved["token"] == "access-new"
    assert saved["refresh_token"] == "refresh-1"
    assert saved["account"] == "sim@example.com"
    assert set(saved["scopes"]) == {DOCS, USERINFO_SCOPE}
    assert oct(home.user_token_path("sim@example.com").stat().st_mode & 0o777) == "0o600"
    assert home.user_access_token("sim") == "access-new"


def test_refresh_failure_hints_login(home, monkeypatch):
    from google.auth.exceptions import RefreshError

    write(home.user_token_path("sim@example.com"), token(expiry="2020-01-01T00:00:00Z"))
    monkeypatch.setattr("google.oauth2.credentials.Credentials.refresh",
                        lambda self, request: (_ for _ in ()).throw(RefreshError("invalid_grant")))
    with pytest.raises(CredentialStoreError) as exc:
        home.user_credentials("sim")
    assert "invalid_grant" in str(exc.value)
    assert exc.value.hint.startswith("megaton auth login --account sim@example.com --scopes docs")


class FakeFlow:
    def __init__(self, client, scopes, email):
        self.client, self.scopes, self.email = client, scopes, email
        self.calls = []

    def run_local_server(self, **kwargs):
        self.calls.append(kwargs)
        from google.oauth2.credentials import Credentials

        return Credentials(
            token="access-login", refresh_token="refresh-login",
            token_uri="https://oauth2.googleapis.com/token", client_id="cid", client_secret="s",
            scopes=self.scopes, expiry=datetime.now(timezone.utc).replace(tzinfo=None) + timedelta(hours=1),
        )


def test_login_user_merges_scopes_and_verifies_identity(home, tmp_path):
    client = tmp_path / "client.json"
    client.write_text(json.dumps({"installed": {"client_id": "cid", "client_secret": "s",
                                                "redirect_uris": ["http://localhost"]}}))
    write(home.user_token_path("sim@example.com"), token(scopes=[DOCS]))
    flows = []

    def factory(client_file, scopes):
        flows.append(FakeFlow(client_file, scopes, "sim@example.com"))
        return flows[-1]

    home._identity_fn = lambda token: {"email": "sim@example.com"}
    entry = home.login_user("sim@example.com", "drive", client_path=client, flow_factory=factory)
    assert flows[0].scopes == [DOCS, DRIVE, USERINFO_SCOPE]
    assert flows[0].calls[0]["login_hint"] == "sim@example.com"
    assert flows[0].calls[0]["prompt"] == "consent"
    assert entry.email == "sim@example.com"
    saved = json.loads(entry.path.read_text())
    assert saved["refresh_token"] == "refresh-login"
    assert set(saved["scopes"]) == {DOCS, DRIVE, USERINFO_SCOPE}


def test_login_user_rejects_other_account_and_keeps_old_token(home, tmp_path):
    home.set_oauth_client(_client_file(tmp_path))
    write(home.user_token_path("sim@example.com"), token())
    home._identity_fn = lambda token: {"email": "someone@else.com"}
    factory = lambda client_file, scopes: FakeFlow(client_file, scopes, "x")  # noqa: E731
    with pytest.raises(CredentialStoreError, match="signed in as someone@else.com"):
        home.login_user("sim@example.com", "docs", flow_factory=factory)
    assert json.loads(home.user_token_path("sim@example.com").read_text())["refresh_token"] == "refresh-1"
    assert not home.user_token_path("someone@else.com").exists()


def test_login_user_requires_alias_or_email_and_oauth_client(home):
    with pytest.raises(CredentialStoreError, match="not an email"):
        home.login_user("me", "docs")
    with pytest.raises(CredentialStoreError) as exc:
        home.login_user("sim@example.com", "docs")
    assert "import --oauth-client" in exc.value.hint


def _client_file(tmp_path):
    client = tmp_path / "client.json"
    client.write_text(json.dumps({"installed": {"client_id": "cid", "client_secret": "s"}}))
    return client


def test_import_user_token_uses_verified_identity(home, tmp_path):
    source = tmp_path / "old_token.json"
    write(source, {k: v for k, v in token().items() if k != "account"})
    home._identity_fn = lambda token: {"email": "sim@example.com"}
    entry = home.import_user_token(source)
    assert entry.path == home.user_token_path("sim@example.com")
    with pytest.raises(CredentialStoreError, match="belongs to sim@example.com"):
        home.import_user_token(source, email="other@example.com")


def test_users_lists_aliases_and_scopes(home):
    write(home.user_token_path("sim@example.com"), token())
    write(home.users_dir / "junk.json", {"not": "a token"})
    home.registry_path.write_text('[users]\nme = "sim@example.com"\n', encoding="utf-8")
    [entry] = home.users()
    assert entry.aliases == ["me"]
    assert entry.to_dict()["scopes"] == ["docs", "userinfo.email"]


# ---- service accounts -----------------------------------------------------

def test_service_account_lookup_by_alias_stem_and_account_id(home, tmp_path):
    source = tmp_path / "sa_proj_sheets-api.json"
    write(source, sa_key())
    entry = home.import_service_account(source)
    assert entry.name == "sa_proj_sheets-api"
    home.registry_path.write_text('[sa]\nsheets = "sa_proj_sheets-api.json"\n', encoding="utf-8")
    assert home.service_account_path("sheets") == entry.path
    assert home.service_account_path("sa_proj_sheets-api") == entry.path
    assert home.service_account_path("sheets-api") == entry.path
    with pytest.raises(CredentialStoreError, match="known: sa_proj_sheets-api, sheets"):
        home.service_account_path("missing")
    assert home.service_accounts()[0].aliases == ["sheets"]


def test_service_account_credentials_requires_scopes(home, tmp_path, monkeypatch):
    source = tmp_path / "k.json"
    write(source, sa_key())
    home.import_service_account(source, name="bq")
    with pytest.raises(CredentialStoreError, match="scopes"):
        home.service_account_credentials("bq", [])
    captured = {}
    monkeypatch.setattr("google.oauth2.service_account.Credentials.from_service_account_file",
                        lambda path, scopes: captured.update(path=path, scopes=scopes) or "creds")
    assert home.service_account_credentials("bq", "sheets,drive") == "creds"
    assert captured == {"path": str(home.sa_dir / "bq.json"), "scopes": [SHEETS, DRIVE]}


def test_registry_template_and_oauth_client_default(home, tmp_path):
    assert home.registry_path.read_text().startswith("# megaton credential store registry")
    assert home.oauth_client_path == home.home / "oauth-client.json"
    saved = home.set_oauth_client(_client_file(tmp_path))
    assert saved == home.oauth_client_path
    bad = tmp_path / "bad.json"
    bad.write_text("{}")
    with pytest.raises(CredentialStoreError, match="not an OAuth client"):
        home.set_oauth_client(bad)


def test_check_reports_ok_and_failures(home, monkeypatch):
    write(home.user_token_path("ok@example.com"), token("ok@example.com"))
    write(home.user_token_path("bad@example.com"), token("bad@example.com", expiry="2020-01-01T00:00:00Z"))

    def fake_refresh(self, creds, email):
        raise CredentialStoreError("refresh failed for bad", hint="megaton auth login --account bad@example.com")

    monkeypatch.setattr(CredentialStore, "_refresh", fake_refresh)
    results = {r["email"]: r for r in home.check()}
    assert results["ok@example.com"]["ok"] is True
    assert results["bad@example.com"]["ok"] is False
    assert results["bad@example.com"]["hint"].startswith("megaton auth login")
    assert [r["email"] for r in home.check("ok@example.com")] == ["ok@example.com"]


# ---- CLI ------------------------------------------------------------------

def test_cli_auth_init_list_path_and_token(home, tmp_path, capsys, monkeypatch):
    assert cli.main(["auth", "init"]) == 0
    write(home.user_token_path("sim@example.com"), token())
    write(tmp_path / "k.json", sa_key())
    assert cli.main(["auth", "import", "--sa", str(tmp_path / "k.json"), "--name", "sheets"]) == 0
    capsys.readouterr()
    assert cli.main(["auth", "list", "--json"]) == 0
    listing = json.loads(capsys.readouterr().out)
    assert listing["users"][0]["email"] == "sim@example.com"
    assert listing["service_accounts"][0]["name"] == "sheets"
    assert cli.main(["auth", "list"]) == 0
    text = capsys.readouterr().out
    assert "sim@example.com: docs, userinfo.email" in text
    assert "sheets: sheets-api@proj.iam.gserviceaccount.com" in text
    assert cli.main(["auth", "path", "--sa", "sheets"]) == 0
    assert capsys.readouterr().out.strip() == str(home.sa_dir / "sheets.json")
    assert cli.main(["auth", "token", "--account", "sim"]) == 0
    assert capsys.readouterr().out.strip() == "access-old"
    assert cli.main(["auth", "token", "--account", "sim", "--scopes", "drive"]) == 1
    err = capsys.readouterr().err
    assert "lacks scopes: drive" in err and "megaton auth login --account sim@example.com --scopes drive" in err
    assert cli.main(["auth", "scopes"]) == 0
    assert "docs" in capsys.readouterr().out


def test_cli_auth_check_exit_code(home, capsys, monkeypatch):
    assert cli.main(["auth", "check"]) == 0
    assert "no user tokens" in capsys.readouterr().out
    write(home.user_token_path("sim@example.com"), token(expiry="2020-01-01T00:00:00Z"))
    monkeypatch.setattr(CredentialStore, "_refresh", lambda self, creds, email: (_ for _ in ()).throw(
        CredentialStoreError("refresh failed", hint="megaton auth login --account sim@example.com")))
    assert cli.main(["auth", "check", "--json"]) == 1
    [status] = json.loads(capsys.readouterr().out)
    assert status["ok"] is False and status["hint"].startswith("megaton auth login")


def test_cli_query_sa_resolves_store_path(home, tmp_path, capsys, monkeypatch):
    write(tmp_path / "k.json", sa_key())
    cli.main(["auth", "import", "--sa", str(tmp_path / "k.json"), "--name", "bq"])
    capsys.readouterr()
    params = tmp_path / "q.json"
    params.write_text(json.dumps({"schema_version": "1.0", "source": "bigquery", "project_id": "p",
                                  "sql": "SELECT 1", "location": "US"}))
    assert cli.main(["query", "--params", str(params), "--sa", "bq", "--validate-only"]) == 0
    assert json.loads(capsys.readouterr().out)["validated"] is True
    assert cli.main(["query", "--params", str(params), "--sa", "missing", "--validate-only"]) == 1
    payload = json.loads(capsys.readouterr().out)
    assert payload["error_code"] == "EXECUTION_FAILED" and payload["error_type"] == "QueryAuthenticationError"
    assert cli.main(["query", "--params", str(params), "--sa", "bq", "--credential", "x", "--validate-only"]) == 2


# ---- Megaton facade ---------------------------------------------------------

def test_megaton_auth_from_store(monkeypatch, home, tmp_path):
    from megaton import start

    write(tmp_path / "k.json", sa_key())
    home.import_service_account(tmp_path / "k.json", name="sheets")
    write(home.user_token_path("sim@example.com"), token())
    monkeypatch.setattr("google.oauth2.service_account.Credentials.from_service_account_file",
                        lambda path, scopes: SimpleNamespace(scopes=scopes, path=path))
    built = []
    monkeypatch.setattr(start.Megaton, "_build_ga_clients", lambda self: built.append(self.json))

    mg = start.Megaton(sa="sheets", headless=True, scopes=["sheets", "drive"])
    assert mg.json == "<store:sa:sheets>"
    assert mg.creds.scopes == [SHEETS, DRIVE]
    assert built == ["<store:sa:sheets>"]

    mg = start.Megaton(account="sim", headless=True)
    assert mg.json == "<store:user:sim@example.com>"
    assert mg.creds.token == "access-old"
    assert built == ["<store:sa:sheets>"]  # no analytics scope → GA clients skipped

    with pytest.raises(ValueError):
        start.Megaton(account="sim", sa="sheets", headless=True)


# ---- review fixes -----------------------------------------------------------

def test_import_user_token_refuses_unverifiable_owner(home, tmp_path):
    source = tmp_path / "foreign.json"
    write(source, token("x@example.com", scopes=[DOCS]))

    def failing_identity(access_token):
        raise RuntimeError("HTTP 401")

    home._identity_fn = failing_identity
    with pytest.raises(CredentialStoreError, match="cannot verify the owner") as exc:
        home.import_user_token(source, email="claimed@example.com")
    assert exc.value.hint.startswith("megaton auth login --account claimed@example.com")
    assert not home.user_token_path("claimed@example.com").exists()


def test_login_saves_granted_scopes_and_cli_warns(home, tmp_path, capsys, monkeypatch):
    home.set_oauth_client(_client_file(tmp_path))
    home._identity_fn = lambda token: {"email": "sim@example.com"}

    class PartialFlow(FakeFlow):
        def run_local_server(self, **kwargs):
            creds = super().run_local_server(**kwargs)
            creds._granted_scopes = ["openid", DOCS, USERINFO_SCOPE]  # user unticked drive
            return creds

    factory = lambda client_file, scopes: PartialFlow(client_file, scopes, "sim@example.com")  # noqa: E731
    entry = home.login_user("sim@example.com", "docs,drive", flow_factory=factory)
    assert set(entry.scopes) == {"openid", DOCS, USERINFO_SCOPE}
    with pytest.raises(CredentialStoreError, match="lacks scopes: drive"):
        home.user_credentials("sim", "drive")

    original = CredentialStore.login_user
    monkeypatch.setattr(CredentialStore, "login_user",
                        lambda self, *a, **k: original(self, *a, flow_factory=factory, **k))
    monkeypatch.setattr(CredentialStore, "identity", lambda self, creds: "sim@example.com")
    assert cli.main(["auth", "login", "--account", "sim@example.com", "--scopes", "docs,drive"]) == 1
    assert "not granted" in capsys.readouterr().err


def test_login_restores_oauthlib_env(home, tmp_path, monkeypatch):
    home.set_oauth_client(_client_file(tmp_path))
    home._identity_fn = lambda token: {"email": "sim@example.com"}
    monkeypatch.delenv("OAUTHLIB_RELAX_TOKEN_SCOPE", raising=False)
    seen = {}

    class EnvFlow(FakeFlow):
        def run_local_server(self, **kwargs):
            import os
            seen["during"] = os.environ.get("OAUTHLIB_RELAX_TOKEN_SCOPE")
            return super().run_local_server(**kwargs)

    home.login_user("sim@example.com", "docs",
                    flow_factory=lambda c, s: EnvFlow(c, s, "sim@example.com"))
    import os
    assert seen["during"] == "1" and "OAUTHLIB_RELAX_TOKEN_SCOPE" not in os.environ


def test_write_is_atomic_and_private(home, monkeypatch):
    path = home.user_token_path("sim@example.com")
    write(path, token())
    monkeypatch.setattr(store_mod.os, "replace", lambda src, dst: (_ for _ in ()).throw(OSError("disk full")))
    with pytest.raises(OSError):
        home._save_user("sim@example.com", home.user_credentials("sim", refresh=False))
    assert json.loads(path.read_text())["token"] == "access-old"  # old file untouched
    assert not list(home.users_dir.glob(".*.tmp"))
    monkeypatch.undo()
    home._save_user("sim@example.com", home.user_credentials("sim", refresh=False))
    assert oct(path.stat().st_mode & 0o777) == "0o600" and not list(home.users_dir.glob(".*.tmp"))


def test_names_cannot_escape_the_store(home):
    for bad in ("../x", "a/b", "..", ".hidden"):
        with pytest.raises(CredentialStoreError, match="invalid"):
            home.user_token_path(bad)
        with pytest.raises(CredentialStoreError, match="invalid"):
            home.service_account_path(bad)


def test_oauth_client_import_honours_registry_name(home, tmp_path):
    home.registry_path.write_text('[oauth]\nclient = "clients/desktop.json"\n', encoding="utf-8")
    saved = home.set_oauth_client(_client_file(tmp_path))
    assert saved == home.home / "clients" / "desktop.json" == home.oauth_client_path
    assert saved.is_file()
