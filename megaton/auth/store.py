"""Machine-wide Google credential store, looked up by identity.

One directory per machine (``~/.config/megaton`` or ``$MEGATON_HOME``) holds every
Google credential a person uses from Python, the ``megaton`` CLI, or an AI agent:

    credentials.toml    aliases for users and service accounts, default OAuth client
    oauth-client.json   installed-app OAuth client used for new user logins
    users/<email>.json  authorized-user tokens (refresh token + granted scopes)
    sa/<name>.json      service-account keys

Callers name an identity (``account="sim"``, ``sa="ierae-sheets"``) instead of a
file path, so the same code works in any repository and on any machine where the
store exists. User tokens carry Workspace scopes only (Docs, Drive, Sheets, Gmail,
GA4, GSC); Google Cloud access goes through service accounts, which never expire.
"""

from __future__ import annotations

import json
import os
import tomllib
import urllib.request
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Iterable, Optional

HOME_ENV = "MEGATON_HOME"
DEFAULT_HOME = "~/.config/megaton"
REGISTRY_FILE = "credentials.toml"
OAUTH_CLIENT_FILE = "oauth-client.json"
USERS_DIR = "users"
SA_DIR = "sa"
USERINFO_SCOPE = "https://www.googleapis.com/auth/userinfo.email"
USERINFO_URL = "https://www.googleapis.com/oauth2/v3/userinfo"

_G = "https://www.googleapis.com/auth/"
SCOPE_ALIASES: dict[str, str] = {
    "docs": _G + "documents",
    "docs.readonly": _G + "documents.readonly",
    "drive": _G + "drive",
    "drive.file": _G + "drive.file",
    "drive.readonly": _G + "drive.readonly",
    "sheets": _G + "spreadsheets",
    "sheets.readonly": _G + "spreadsheets.readonly",
    "gmail": _G + "gmail.modify",
    "gmail.readonly": _G + "gmail.readonly",
    "gmail.modify": _G + "gmail.modify",
    "gmail.compose": _G + "gmail.compose",
    "gmail.send": _G + "gmail.send",
    "gmail.labels": _G + "gmail.labels",
    "gmail.full": "https://mail.google.com/",
    "calendar": _G + "calendar",
    "calendar.readonly": _G + "calendar.readonly",
    "analytics": _G + "analytics.readonly",
    "analytics.edit": _G + "analytics.edit",
    "webmasters": _G + "webmasters.readonly",
    "webmasters.edit": _G + "webmasters",
    "tagmanager": _G + "tagmanager.readonly",
    "tagmanager.edit": _G + "tagmanager.edit.containers",
    "tagmanager.versions": _G + "tagmanager.edit.containerversions",
    "tagmanager.publish": _G + "tagmanager.publish",
    "bigquery": _G + "bigquery",
    "bigquery.readonly": _G + "bigquery.readonly",
    "cloud-platform": _G + "cloud-platform",
    "userinfo.email": USERINFO_SCOPE,
    "email": USERINFO_SCOPE,
    # Google adds "openid" to grants that include userinfo.email; accept it so stored
    # scopes and the hints built from them can be passed back to `megaton auth login`.
    "openid": "openid",
}

# A granted scope on the left also satisfies every scope on the right.
_SCOPE_IMPLIES: dict[str, set[str]] = {
    "https://mail.google.com/": {
        _G + s for s in ("gmail.readonly", "gmail.modify", "gmail.compose", "gmail.send",
                         "gmail.labels", "gmail.metadata", "gmail.insert")
    },
    _G + "gmail.modify": {
        _G + s for s in ("gmail.readonly", "gmail.compose", "gmail.send", "gmail.labels",
                         "gmail.metadata", "gmail.insert")
    },
    _G + "gmail.compose": {_G + "gmail.send"},
    _G + "gmail.readonly": {_G + "gmail.metadata"},
    _G + "drive": {_G + "drive.file", _G + "drive.readonly", _G + "spreadsheets",
                   _G + "spreadsheets.readonly", _G + "documents", _G + "documents.readonly"},
    _G + "drive.readonly": {_G + "spreadsheets.readonly", _G + "documents.readonly"},
    _G + "spreadsheets": {_G + "spreadsheets.readonly"},
    _G + "documents": {_G + "documents.readonly"},
    _G + "webmasters": {_G + "webmasters.readonly"},
    _G + "tagmanager.edit.containers": {_G + "tagmanager.readonly"},
    _G + "calendar": {_G + "calendar.readonly"},
    _G + "bigquery": {_G + "bigquery.readonly"},
    _G + "cloud-platform": {_G + "bigquery", _G + "bigquery.readonly"},
}

REGISTRY_TEMPLATE = """# megaton credential store registry (edit by hand).
# Aliases let code and agents name an identity instead of a file.

[oauth]
# Installed-app OAuth client used by `megaton auth login` (relative to this directory).
client = "oauth-client.json"

[users]
# alias = "email"; the token lives at users/<email>.json
# sim = "sim@example.com"

[sa]
# alias = "file name under sa/"
# project-sheets = "sa_project_sheets-api.json"
"""


class CredentialStoreError(RuntimeError):
    """A credential could not be resolved; ``hint`` is the command that fixes it."""

    def __init__(self, message: str, hint: Optional[str] = None):
        super().__init__(message)
        self.hint = hint

    def __str__(self) -> str:  # pragma: no cover - trivial
        base = super().__str__()
        return f"{base}\n  → {self.hint}" if self.hint else base


def expand_scopes(scopes: Optional[Iterable[str] | str]) -> list[str]:
    """Turn aliases (``docs``, ``gmail.readonly``) or a comma list into full scope URLs."""
    if scopes is None:
        return []
    if isinstance(scopes, str):
        scopes = [s for s in scopes.replace(" ", ",").split(",") if s]
    out: list[str] = []
    for scope in scopes:
        scope = scope.strip()
        if not scope:
            continue
        url = SCOPE_ALIASES.get(scope)
        if url is None:
            if not scope.startswith("https://"):
                raise CredentialStoreError(
                    f"unknown scope alias: {scope}",
                    hint="use one of: " + ", ".join(sorted(SCOPE_ALIASES)),
                )
            url = scope
        if url not in out:
            out.append(url)
    return out


def scopes_satisfied(granted: Iterable[str], required: Iterable[str]) -> list[str]:
    """Return the required scopes that the granted set does not cover."""
    available = set(granted)
    for scope in list(available):
        available.update(_SCOPE_IMPLIES.get(scope, ()))
    return [s for s in required if s not in available]


def short_scope(url: str) -> str:
    """Prefer the alias for display (``docs``) and fall back to the URL tail."""
    for alias, full in SCOPE_ALIASES.items():
        if full == url and alias not in ("email",):
            return alias
    return url.rsplit("/", 1)[-1]


@dataclass
class UserEntry:
    email: str
    path: Path
    scopes: list[str] = field(default_factory=list)
    client_id: str = ""
    expiry: Optional[str] = None
    aliases: list[str] = field(default_factory=list)

    def to_dict(self) -> dict:
        return {
            "email": self.email,
            "aliases": self.aliases,
            "path": str(self.path),
            "scopes": [short_scope(s) for s in self.scopes],
            "client_id": self.client_id,
            "expiry": self.expiry,
        }


@dataclass
class ServiceAccountEntry:
    name: str
    path: Path
    client_email: str = ""
    project_id: str = ""
    aliases: list[str] = field(default_factory=list)

    def to_dict(self) -> dict:
        return {
            "name": self.name,
            "aliases": self.aliases,
            "path": str(self.path),
            "client_email": self.client_email,
            "project_id": self.project_id,
        }


def _fetch_identity(access_token: str) -> dict:
    request = urllib.request.Request(USERINFO_URL, headers={"Authorization": f"Bearer {access_token}"})
    with urllib.request.urlopen(request, timeout=30) as response:  # noqa: S310 - fixed Google URL
        return json.loads(response.read().decode("utf-8"))


def _write_private(path: Path, text: str) -> None:
    """Write atomically (temp file + rename) with mode 0600 so readers never see a partial file."""
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    fd = os.open(tmp, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            handle.write(text)
            handle.flush()
            os.fsync(handle.fileno())
        os.chmod(tmp, 0o600)
        os.replace(tmp, path)
    except BaseException:
        tmp.unlink(missing_ok=True)
        raise


def _safe_name(value: str, what: str) -> str:
    """Reject names that could escape the store directory."""
    value = (value or "").strip()
    if not value or value in (".", "..") or "/" in value or "\\" in value or value.startswith("."):
        raise CredentialStoreError(f"invalid {what}: {value!r}")
    return value


class CredentialStore:
    """Resolve Google credentials by identity from the machine-wide store."""

    def __init__(self, home: Optional[str | Path] = None, *,
                 identity_fn: Callable[[str], dict] = _fetch_identity):
        raw = home or os.environ.get(HOME_ENV) or DEFAULT_HOME
        self.home = Path(raw).expanduser()
        self._identity_fn = identity_fn

    # ---- layout -----------------------------------------------------------
    @property
    def users_dir(self) -> Path:
        return self.home / USERS_DIR

    @property
    def sa_dir(self) -> Path:
        return self.home / SA_DIR

    @property
    def registry_path(self) -> Path:
        return self.home / REGISTRY_FILE

    def exists(self) -> bool:
        return self.home.is_dir()

    def init(self) -> Path:
        """Create the directory layout and a commented registry; idempotent."""
        self.home.mkdir(parents=True, exist_ok=True)
        os.chmod(self.home, 0o700)
        for sub in (self.users_dir, self.sa_dir):
            sub.mkdir(exist_ok=True)
            os.chmod(sub, 0o700)
        if not self.registry_path.exists():
            _write_private(self.registry_path, REGISTRY_TEMPLATE)
        return self.home

    def registry(self) -> dict[str, Any]:
        if not self.registry_path.is_file():
            return {}
        try:
            with open(self.registry_path, "rb") as handle:
                data = tomllib.load(handle)
        except tomllib.TOMLDecodeError as exc:
            raise CredentialStoreError(f"{self.registry_path} is not valid TOML: {exc}") from exc
        return data if isinstance(data, dict) else {}

    @property
    def oauth_client_path(self) -> Path:
        name = str((self.registry().get("oauth") or {}).get("client") or OAUTH_CLIENT_FILE)
        path = Path(name).expanduser()
        return path if path.is_absolute() else self.home / path

    def set_oauth_client(self, path: str | Path) -> Path:
        source = Path(path).expanduser()
        info = json.loads(source.read_text(encoding="utf-8"))
        if not isinstance(info, dict) or not ({"installed", "web"} & set(info)):
            raise CredentialStoreError(f"{source} is not an OAuth client file")
        self.init()
        target = self.oauth_client_path  # honours [oauth].client in the registry
        _write_private(target, json.dumps(info, ensure_ascii=False, indent=2) + "\n")
        return target

    # ---- users ------------------------------------------------------------
    def _user_aliases(self) -> dict[str, str]:
        users = self.registry().get("users") or {}
        return {str(k): str(v) for k, v in users.items() if isinstance(v, str)}

    def resolve_email(self, account: str) -> str:
        """Alias or email → email. Without an alias, a unique local part also matches."""
        account = (account or "").strip()
        if not account:
            raise CredentialStoreError("account is required")
        aliases = self._user_aliases()
        if account in aliases:
            return aliases[account]
        if "@" in account:
            return account
        matches = sorted({e.email for e in self.users() if e.email.split("@", 1)[0] == account})
        if len(matches) == 1:
            return matches[0]
        if not matches:
            raise CredentialStoreError(
                f"no user token for '{account}' in {self.users_dir}",
                hint=f"megaton auth login --account <email> --scopes docs,drive  (then alias it as "
                     f"'{account}' in {self.registry_path})",
            )
        raise CredentialStoreError(
            f"'{account}' matches several users: {', '.join(matches)}",
            hint=f"add an alias in {self.registry_path} or pass the full email",
        )

    def user_token_path(self, email: str) -> Path:
        return self.users_dir / f"{_safe_name(email, 'email')}.json"

    def _load_token(self, email: str) -> dict:
        path = self.user_token_path(email)
        if not path.is_file():
            raise CredentialStoreError(
                f"no token for {email} ({path})",
                hint=f"megaton auth login --account {email} --scopes docs,drive",
            )
        info = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(info, dict) or not info.get("refresh_token"):
            raise CredentialStoreError(
                f"{path} is not an authorized-user token with a refresh token",
                hint=f"megaton auth login --account {email} --scopes docs,drive",
            )
        return info

    @staticmethod
    def _token_scopes(info: dict) -> list[str]:
        scopes = info.get("scopes") or []
        if isinstance(scopes, str):
            scopes = scopes.split()
        return [str(s) for s in scopes]

    def users(self) -> list[UserEntry]:
        if not self.users_dir.is_dir():
            return []
        alias_map: dict[str, list[str]] = {}
        for alias, email in self._user_aliases().items():
            alias_map.setdefault(email, []).append(alias)
        entries = []
        for path in sorted(self.users_dir.glob("*.json")):
            try:
                info = json.loads(path.read_text(encoding="utf-8"))
            except (OSError, ValueError):
                continue
            if not isinstance(info, dict) or "refresh_token" not in info:
                continue
            email = str(info.get("account") or path.stem)
            entries.append(UserEntry(
                email=email, path=path, scopes=self._token_scopes(info),
                client_id=str(info.get("client_id", "")), expiry=info.get("expiry"),
                aliases=alias_map.get(email, []),
            ))
        return entries

    def user_credentials(self, account: str, scopes: Optional[Iterable[str] | str] = None,
                         *, refresh: bool = True):
        """Load a user's credentials; refresh and persist the access token when expired.

        ``scopes`` (aliases or URLs) are checked against the token's granted scopes and
        never expand the grant; missing scopes raise with the login command to run.
        """
        from google.oauth2.credentials import Credentials

        email = self.resolve_email(account)
        info = self._load_token(email)
        granted = self._token_scopes(info)
        required = expand_scopes(scopes)
        self._require_scopes(email, granted, required)
        creds = Credentials.from_authorized_user_info(info, scopes=granted or None)
        if refresh and not creds.valid:
            self._refresh(creds, email)
            # Persist what Google now reports before judging it, so the stored grant
            # stays truthful even when this call fails for a narrowed grant.
            self._save_user(email, creds, info)
            actual = getattr(creds, "granted_scopes", None)
            if actual is not None:
                self._require_scopes(email, actual, required)
        return creds

    @staticmethod
    def _require_scopes(email: str, granted: Iterable[str], required: list[str]) -> None:
        missing = scopes_satisfied(granted, required)
        if missing:
            wanted = ",".join(short_scope(s) for s in required)
            raise CredentialStoreError(
                f"token for {email} lacks scopes: {', '.join(short_scope(s) for s in missing)}",
                hint=f"megaton auth login --account {email} --scopes {wanted}",
            )

    def _refresh(self, creds, email: str) -> None:
        from google.auth.exceptions import RefreshError
        from google.auth.transport.requests import Request

        try:
            creds.refresh(Request())
        except RefreshError as exc:
            raise CredentialStoreError(
                f"refresh failed for {email}: {exc}",
                hint=f"megaton auth login --account {email} --scopes "
                     + (",".join(short_scope(s) for s in (creds.scopes or [])) or "docs,drive"),
            ) from exc

    def _save_user(self, email: str, creds, base: Optional[dict] = None) -> Path:
        info = dict(base or {})
        info.update(json.loads(creds.to_json()))
        info["account"] = email
        # Record what Google actually granted (the consent screen lets users untick scopes),
        # not what was requested; the requested list is what Credentials.to_json() writes.
        granted = getattr(creds, "granted_scopes", None)
        info["scopes"] = list(granted if granted is not None else (creds.scopes or []))
        path = self.user_token_path(email)
        _write_private(path, json.dumps(info, ensure_ascii=False, indent=2) + "\n")
        return path

    def user_access_token(self, account: str, scopes: Optional[Iterable[str] | str] = None) -> str:
        creds = self.user_credentials(account, scopes)
        return str(creds.token)

    def identity(self, creds) -> str:
        """Email the credentials belong to (needs ``userinfo.email``)."""
        if not creds.valid:
            self._refresh(creds, getattr(creds, "account", "") or "user")
        info = self._identity_fn(str(creds.token))
        email = str(info.get("email") or "")
        if not email:
            raise CredentialStoreError("could not read the account email from Google (userinfo.email scope missing?)")
        return email

    def login_user(self, account: str, scopes: Iterable[str] | str, *,
                   client_path: Optional[str | Path] = None, port: int = 0,
                   open_browser: bool = True, flow_factory: Optional[Callable[..., Any]] = None) -> UserEntry:
        """Run the installed-app OAuth flow and store the token under the verified email.

        Scopes are merged with the token's existing grant so one token per user grows
        incrementally. The signed-in Google account must match ``account``.
        """
        account = (account or "").strip()
        aliases = self._user_aliases()
        email = aliases.get(account) or (account if "@" in account else None)
        if not email:
            raise CredentialStoreError(
                f"'{account}' is not an email or a registered alias",
                hint=f"pass the email, or add '{account} = \"<email>\"' under [users] in {self.registry_path}",
            )
        requested = expand_scopes(scopes)
        if not requested:
            raise CredentialStoreError("at least one scope is required", hint="--scopes docs,drive")
        existing: list[str] = []
        path = self.user_token_path(email)
        if path.is_file():
            try:
                existing = self._token_scopes(json.loads(path.read_text(encoding="utf-8")))
            except (OSError, ValueError):
                existing = []
        merged = list(dict.fromkeys([*existing, *requested, USERINFO_SCOPE]))
        client = Path(client_path).expanduser() if client_path else self.oauth_client_path
        if not client.is_file():
            raise CredentialStoreError(
                f"OAuth client not found: {client}",
                hint="megaton auth import --oauth-client <client_secret.json>",
            )
        if flow_factory is None:
            from google_auth_oauthlib.flow import InstalledAppFlow

            def flow_factory(client_file, scopes):  # noqa: E306
                return InstalledAppFlow.from_client_secrets_file(str(client_file), scopes=scopes)
        flow = flow_factory(client, merged)
        # Google adds "openid" to the granted set; oauthlib treats that as an error unless relaxed.
        # The granted set is recorded by _save_user, so the relaxed check loses nothing.
        previous = os.environ.get("OAUTHLIB_RELAX_TOKEN_SCOPE")
        os.environ["OAUTHLIB_RELAX_TOKEN_SCOPE"] = "1"
        try:
            creds = flow.run_local_server(port=port, open_browser=open_browser, login_hint=email,
                                          prompt="consent", access_type="offline")
        finally:
            if previous is None:
                os.environ.pop("OAUTHLIB_RELAX_TOKEN_SCOPE", None)
            else:
                os.environ["OAUTHLIB_RELAX_TOKEN_SCOPE"] = previous
        signed_in = self.identity(creds)
        if signed_in.casefold() != email.casefold():
            raise CredentialStoreError(
                f"signed in as {signed_in}, expected {email}; token not saved",
                hint=f"megaton auth login --account {email} ... and choose {email} in the browser",
            )
        self.init()
        self._save_user(email, creds)
        return self._user_entry(email)

    def _user_entry(self, email: str) -> UserEntry:
        for entry in self.users():
            if entry.email == email:
                return entry
        raise CredentialStoreError(f"token for {email} was not saved")

    def import_user_token(self, path: str | Path, email: Optional[str] = None) -> UserEntry:
        """Copy an existing authorized-user token into the store (email verified online)."""
        from google.oauth2.credentials import Credentials

        source = Path(path).expanduser()
        info = json.loads(source.read_text(encoding="utf-8"))
        if not isinstance(info, dict) or not info.get("refresh_token"):
            raise CredentialStoreError(f"{source} is not an authorized-user token")
        granted = self._token_scopes(info)
        creds = Credentials.from_authorized_user_info(info, scopes=granted or None)
        # The owner is always verified online; a token that cannot prove its owner is not imported.
        try:
            signed_in = self.identity(creds)
        except CredentialStoreError:
            raise
        except Exception as exc:  # noqa: BLE001 - HTTP/refresh failures become one actionable error
            raise CredentialStoreError(
                f"cannot verify the owner of {source}: {exc}",
                hint=f"megaton auth login --account {email or '<email>'} --scopes "
                     + (",".join(short_scope(s) for s in granted) or "docs,drive"),
            ) from exc
        if email and signed_in.casefold() != email.casefold():
            raise CredentialStoreError(f"{source} belongs to {signed_in}, not {email}")
        self.init()
        self._save_user(signed_in, creds, info)
        return self._user_entry(signed_in)

    # ---- service accounts -------------------------------------------------
    def _sa_aliases(self) -> dict[str, str]:
        entries = self.registry().get("sa") or {}
        return {str(k): str(v) for k, v in entries.items() if isinstance(v, str)}

    def service_accounts(self) -> list[ServiceAccountEntry]:
        if not self.sa_dir.is_dir():
            return []
        alias_map: dict[str, list[str]] = {}
        for alias, file in self._sa_aliases().items():
            alias_map.setdefault(Path(file).name, []).append(alias)
        entries = []
        for path in sorted(self.sa_dir.glob("*.json")):
            try:
                info = json.loads(path.read_text(encoding="utf-8"))
            except (OSError, ValueError):
                continue
            if not isinstance(info, dict) or info.get("type") != "service_account":
                continue
            entries.append(ServiceAccountEntry(
                name=path.stem, path=path, client_email=str(info.get("client_email", "")),
                project_id=str(info.get("project_id", "")), aliases=alias_map.get(path.name, []),
            ))
        return entries

    def service_account_path(self, name: str) -> Path:
        """Alias, file stem, file name, or unique service-account id → key file path."""
        name = _safe_name(name, "service account name")
        aliases = self._sa_aliases()
        if name in aliases:
            candidate = Path(aliases[name]).expanduser()
            path = candidate if candidate.is_absolute() else self.sa_dir / candidate
            if path.is_file():
                return path
            raise CredentialStoreError(f"alias '{name}' points to a missing file: {path}")
        for candidate in (self.sa_dir / name, self.sa_dir / f"{name}.json"):
            if candidate.is_file():
                return candidate
        matches = [e for e in self.service_accounts() if e.client_email.split("@", 1)[0] == name]
        if len(matches) == 1:
            return matches[0].path
        known = ", ".join(sorted({*aliases, *(e.name for e in self.service_accounts())})) or "(none)"
        raise CredentialStoreError(
            f"no service account '{name}' in {self.sa_dir}; known: {known}",
            hint="megaton auth import --sa <key.json> --name <name>",
        )

    def service_account_credentials(self, name: str, scopes: Iterable[str] | str):
        from google.oauth2 import service_account

        path = self.service_account_path(name)
        expanded = expand_scopes(scopes)
        if not expanded:
            raise CredentialStoreError("service account scopes are required", hint="scopes=['sheets','drive']")
        return service_account.Credentials.from_service_account_file(str(path), scopes=expanded)

    def import_service_account(self, path: str | Path, name: Optional[str] = None) -> ServiceAccountEntry:
        source = Path(path).expanduser()
        info = json.loads(source.read_text(encoding="utf-8"))
        if not isinstance(info, dict) or info.get("type") != "service_account":
            raise CredentialStoreError(f"{source} is not a service account key")
        self.init()
        target = self.sa_dir / (f"{_safe_name(name, 'service account name')}.json" if name
                                else _safe_name(source.name, "file name"))
        _write_private(target, json.dumps(info, ensure_ascii=False, indent=2) + "\n")
        for entry in self.service_accounts():
            if entry.path == target:
                return entry
        raise CredentialStoreError(f"{target} was not saved")  # pragma: no cover

    # ---- status -----------------------------------------------------------
    def check(self, account: Optional[str] = None) -> list[dict]:
        """Refresh each user token (or one) and report validity; never raises per entry."""
        results = []
        entries = self.users()
        if account:
            email = self.resolve_email(account)
            entries = [e for e in entries if e.email == email]
            if not entries:
                return [{
                    "email": email, "aliases": [], "path": str(self.users_dir / f"{email}.json"),
                    "scopes": [], "client_id": "", "expiry": None, "ok": False,
                    "error": f"no user token for {email}",
                    "hint": f"megaton auth login --account {email} --scopes docs,drive",
                }]
        for entry in entries:
            status = entry.to_dict()
            try:
                creds = self.user_credentials(entry.email)
                status.update(ok=True, expiry=creds.expiry.replace(tzinfo=timezone.utc).isoformat()
                              if isinstance(creds.expiry, datetime) else status.get("expiry"))
            except CredentialStoreError as exc:
                status.update(ok=False, error=str(exc).split("\n", 1)[0], hint=exc.hint)
            results.append(status)
        return results


__all__ = [
    "CredentialStore",
    "CredentialStoreError",
    "DEFAULT_HOME",
    "HOME_ENV",
    "SCOPE_ALIASES",
    "ServiceAccountEntry",
    "UserEntry",
    "expand_scopes",
    "scopes_satisfied",
    "short_scope",
]
