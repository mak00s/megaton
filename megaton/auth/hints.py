"""One-line tips that point legacy credential paths at the credential store.

Shown at most once per credential per process, on stderr (stdout stays clean for
JSON CLIs), only after authentication succeeded, and never in CI, where the
machine-wide store does not exist. ``MEGATON_HINTS=0`` silences them. A tip must
never change or break authentication, so every failure here is swallowed.
"""

from __future__ import annotations

import json
import os
import shlex
import sys
from contextlib import contextmanager
from contextvars import ContextVar
from pathlib import Path
from typing import Iterator, Optional

HINTS_ENV = "MEGATON_HINTS"
_OFF = {"0", "false", "no", "off"}
_TRUE = {"1", "true", "yes"}
_shown: set[str] = set()
_suppressed: ContextVar[bool] = ContextVar("megaton_store_hints_suppressed", default=False)


@contextmanager
def suppressed() -> Iterator[None]:
    """Silence tips for credentials a caller selected automatically (e.g. an app router)."""
    token = _suppressed.set(True)
    try:
        yield
    finally:
        _suppressed.reset(token)


def hints_enabled() -> bool:
    if _suppressed.get():
        return False
    if os.environ.get(HINTS_ENV, "").strip().lower() in _OFF:
        return False
    if os.environ.get("CI", "").strip().lower() in _TRUE or os.environ.get("GITHUB_ACTIONS"):
        return False
    return True


def _credential_type(info: dict) -> Optional[str]:
    if info.get("type") == "service_account":
        return "service_account"
    if "installed" in info or "web" in info:
        return "oauth_client"
    return None


def _user_login_scopes() -> str:
    """Scope aliases a legacy OAuth session requests, minus Google Cloud scopes.

    Cloud scopes would put the user token under Google Cloud session control, so
    BigQuery/Cloud access is pointed at a service account instead.
    """
    from ..constants import DEFAULT_SCOPES
    from .store import short_scope

    cloud = {"https://www.googleapis.com/auth/cloud-platform",
             "https://www.googleapis.com/auth/bigquery", "https://www.googleapis.com/auth/bigquery.readonly"}
    return ",".join(dict.fromkeys(short_scope(s) for s in DEFAULT_SCOPES if s not in cloud))


def store_hint(path: str | Path, *, store=None) -> Optional[str]:
    """Return the tip for a credential file, or None when there is nothing better to say."""
    from .store import CredentialStore

    source = Path(path).expanduser()
    info = json.loads(source.read_text(encoding="utf-8"))
    if not isinstance(info, dict):
        return None
    kind = _credential_type(info)
    store = store or CredentialStore()
    if kind == "service_account":
        email = str(info.get("client_email") or "")
        for entry in store.service_accounts():
            if email and entry.client_email == email:
                name = entry.aliases[0] if entry.aliases else entry.name
                return (f'this service account is in the credential store; pass sa="{name}" '
                        f"(CLI: --sa {name}) instead of a file path. See docs/credential-store.md")
        return (f"name this key once with `megaton auth import --sa {shlex.quote(str(source))} --name <name>`, "
                'then pass sa="<name>" (CLI: --sa <name>) from any repository. See docs/credential-store.md')
    if kind == "oauth_client":
        return ("user tokens can live in the credential store: run `megaton auth import --oauth-client "
                f"{shlex.quote(str(source))}` and `megaton auth login --account <email> --scopes "
                f'{_user_login_scopes()}`, then pass account="<email>" (use sa= for BigQuery/Cloud). '
                "See docs/credential-store.md")
    return None


def suggest_store(path: str | Path, *, stream=None) -> bool:
    """Print the tip for ``path`` once; return whether anything was printed."""
    try:
        if not hints_enabled():
            return False
        key = str(Path(path).expanduser().resolve())
        if key in _shown:
            return False
        message = store_hint(path)
        if not message:
            return False
        _shown.add(key)
        print(f"[megaton] hint: {message} (silence with {HINTS_ENV}=0)", file=stream or sys.stderr)
        return True
    except Exception:  # noqa: BLE001 - a tip must never affect authentication
        return False


__all__ = ["HINTS_ENV", "hints_enabled", "store_hint", "suggest_store", "suppressed"]
