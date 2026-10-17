"""`megaton auth` subcommands: manage the machine-wide credential store."""

from __future__ import annotations

import argparse
import json
import sys
from typing import Optional

from .store import SCOPE_ALIASES, CredentialStore, CredentialStoreError, short_scope


def add_parser(commands: argparse._SubParsersAction) -> None:
    auth = commands.add_parser("auth", help="Manage Google credentials in ~/.config/megaton")
    auth.add_argument("--home", help="Store directory (default: $MEGATON_HOME or ~/.config/megaton)")
    sub = auth.add_subparsers(dest="auth_command", required=True)

    sub.add_parser("init", help="Create the store layout and registry template")

    show = sub.add_parser("list", help="Show stored users and service accounts")
    show.add_argument("--json", action="store_true")

    login = sub.add_parser("login", help="Sign in a Google account in the browser and store its token")
    login.add_argument("--account", required=True, help="Email or alias from credentials.toml")
    login.add_argument("--scopes", required=True, help="Comma list, e.g. docs,drive,sheets,gmail.readonly")
    login.add_argument("--client", help="OAuth client JSON (default: the store's oauth-client.json)")
    login.add_argument("--port", type=int, default=0, help="Loopback port (default: random)")
    login.add_argument("--no-browser", action="store_true", help="Print the URL instead of opening a browser")

    token = sub.add_parser("token", help="Print a fresh access token for curl or other tools")
    token.add_argument("--account", required=True)
    token.add_argument("--scopes", help="Fail unless the token already covers these scopes")

    check = sub.add_parser("check", help="Refresh stored user tokens and report validity")
    check.add_argument("--account")
    check.add_argument("--json", action="store_true")

    imp = sub.add_parser("import", help="Copy an existing credential file into the store")
    group = imp.add_mutually_exclusive_group(required=True)
    group.add_argument("--user-token", help="authorized_user token JSON (refresh token)")
    group.add_argument("--sa", help="service account key JSON")
    group.add_argument("--oauth-client", help="installed-app OAuth client JSON")
    imp.add_argument("--email", help="Expected account for --user-token")
    imp.add_argument("--name", help="Store name for --sa (default: source file name)")

    path = sub.add_parser("path", help="Print the file path behind an identity")
    group = path.add_mutually_exclusive_group(required=True)
    group.add_argument("--sa")
    group.add_argument("--account")

    sub.add_parser("scopes", help="List scope aliases")


def run(args: argparse.Namespace) -> int:
    store = CredentialStore(home=args.home)
    try:
        return _dispatch(store, args)
    except CredentialStoreError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1


def _dispatch(store: CredentialStore, args: argparse.Namespace) -> int:
    command = args.auth_command
    if command == "init":
        home = store.init()
        print(f"store ready: {home}")
        print(f"registry:    {store.registry_path}")
        return 0
    if command == "list":
        users = [u.to_dict() for u in store.users()]
        sas = [s.to_dict() for s in store.service_accounts()]
        if args.json:
            print(json.dumps({"home": str(store.home), "users": users, "service_accounts": sas},
                             ensure_ascii=False, indent=2))
            return 0
        print(f"store: {store.home}")
        print("users:")
        for user in users or []:
            alias = f" ({', '.join(user['aliases'])})" if user["aliases"] else ""
            print(f"  {user['email']}{alias}: {', '.join(user['scopes']) or '-'}")
        if not users:
            print("  (none)  → megaton auth login --account <email> --scopes docs,drive")
        print("service accounts:")
        for entry in sas or []:
            alias = f" ({', '.join(entry['aliases'])})" if entry["aliases"] else ""
            print(f"  {entry['name']}{alias}: {entry['client_email']}")
        if not sas:
            print("  (none)  → megaton auth import --sa <key.json>")
        return 0
    if command == "login":
        entry = store.login_user(args.account, args.scopes, client_path=args.client, port=args.port,
                                 open_browser=not args.no_browser)
        print(f"saved {entry.path}")
        print("scopes: " + ", ".join(short_scope(s) for s in entry.scopes))
        return 0
    if command == "token":
        print(store.user_access_token(args.account, args.scopes))
        return 0
    if command == "check":
        results = store.check(args.account)
        if args.json:
            print(json.dumps(results, ensure_ascii=False, indent=2))
        else:
            for status in results:
                mark = "ok " if status.get("ok") else "NG "
                line = f"{mark} {status['email']}: {', '.join(status['scopes']) or '-'}"
                if not status.get("ok"):
                    line += f"\n     {status.get('error')}\n     → {status.get('hint')}"
                print(line)
            if not results:
                print("no user tokens stored")
        return 0 if all(s.get("ok") for s in results) else 1
    if command == "import":
        if args.user_token:
            entry = store.import_user_token(args.user_token, args.email)
            print(f"saved {entry.path}")
        elif args.sa:
            entry = store.import_service_account(args.sa, args.name)
            print(f"saved {entry.path} ({entry.client_email})")
        else:
            print(f"saved {store.set_oauth_client(args.oauth_client)}")
        return 0
    if command == "path":
        if args.sa:
            print(store.service_account_path(args.sa))
        else:
            email = store.resolve_email(args.account)
            path = store.user_token_path(email)
            if not path.is_file():
                raise CredentialStoreError(f"no token for {email}",
                                           hint=f"megaton auth login --account {email} --scopes docs,drive")
            print(path)
        return 0
    if command == "scopes":
        width = max(len(k) for k in SCOPE_ALIASES)
        for alias, url in SCOPE_ALIASES.items():
            print(f"{alias.ljust(width)}  {url}")
        return 0
    raise CredentialStoreError(f"unknown auth command: {command}")  # pragma: no cover


__all__ = ["add_parser", "run"]
