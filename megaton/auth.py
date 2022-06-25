"""
Functions for handling Authentications
"""

import json
import logging
import os
from collections import defaultdict

import google.oauth2.credentials
from google.oauth2 import service_account
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow

_REQUIRED_CONFIG_KEYS = frozenset(("auth_uri", "token_uri", "client_id"))
LOGGER = logging.getLogger(__name__)


def _is_service_account_json(json_file: str):
    """Return true if the provided JSON file is for a service account."""
    with open(json_file, 'r') as f:
        return _is_service_account_key(f.read())


def _is_service_account_key(json_text: str):
    """Return true if the provided text is a JSON service credentials file."""
    try:
        key_obj = json.loads(json_text)
    except json.JSONDecodeError:
        return False
    if not key_obj or key_obj.get('type', '') != 'service_account':
        return False
    return True


def get_client_secrets_type(client_config: dict):
    """Gets a client type from client configuration loaded from a Google-format client secrets file.

    Args:
        client_config (Mapping[str, Any]): The client
            configuration in the Google `client secrets`_ format.

    Returns:
        client_type [str]: The client type, either ``'service_account'`` or ``'web'`` or ``'installed'``
    """
    if client_config.get('type', '') == "service_account":
        return "service_account"
    elif "web" in client_config:
        client_type = "web"
    elif "installed" in client_config:
        client_type = "installed"
    else:
        return None
    config = client_config[client_type]
    if _REQUIRED_CONFIG_KEYS.issubset(config.keys()):
        return client_type


def get_client_secrets_type_from_file(json_file: str):
    """Gets a client type from a Google client secrets file.

        Args:
            json_file (str): The path to the client secrets .json file.

        Returns:
            client_type [str]: The client type, either ``'service_account'`` or ``'web'`` or ``'installed'``
        """
    with open(json_file, "r") as json_file:
        client_config = json.load(json_file)

    return get_client_secrets_type(client_config)


def get_credentials_files_from(json_dir: str):
    """Gets a list of valid credentials json files from a directory recursively"""
    json_files = defaultdict(lambda: {})
    for root, dirs, files in os.walk(json_dir):
        for file in files:
            if file.endswith('.json'):
                client_type = get_client_secrets_type_from_file(os.path.join(root, file))
                if client_type == 'service_account':
                    json_files['Service Account'][file] = os.path.join(root, file)
                elif client_type in ['installed', 'web']:
                    json_files['OAuth'][file] = os.path.join(root, file)
    return json_files


def get_client_secrets_from_dir(json_dir: str):
    """Gets a list of valid client secrets json files from a directory recursively"""
    client_secrets = []
    for root, dirs, files in os.walk(json_dir):
        for file in files:
            if file.endswith('.json'):
                client_type = get_client_secrets_type_from_file(os.path.join(root, file))
                if client_type == 'service_account':
                    client_secrets.append({"type": client_type, "filename": file, "path": os.path.join(root, file)})
                elif client_type in ['installed', 'web']:
                    client_secrets.append({"type": "OAuth", "filename": file, "path": os.path.join(root, file)})

    return client_secrets


def get_cache_filename_from_json(source_file: str):
    """Name cache file based on the provided source file"""
    base_name = os.path.splitext(os.path.basename(source_file))[0]
    return f".{base_name}_cached-cred.json"


def save_credentials(cache_file: str, credentials: Credentials):
    """Save Credentials to cache file
    """
    with open(cache_file, 'w') as w:
        LOGGER.debug(f"saving credentials to {cache_file}")
        w.write(credentials.to_json())
    return credentials


def load_credentials(cache_file: str, scopes: list):
    """Load Credentials from cache file
    """
    if os.path.isfile(cache_file):
        LOGGER.debug(f"loading credentials from {cache_file}")
        return Credentials.from_authorized_user_file(cache_file, scopes=scopes)


def delete_credentials(cache_file: str = "creden-cache.json"):
    """Delete Credentials cache file
    """
    if os.path.isfile(cache_file):
        LOGGER.debug(f"deleting cache file {cache_file}")
        os.remove(cache_file)


def _get_oauth_redirect(client_secret_file: str, scopes: list):
    """Run OAuth2 Flow"""
    flow = InstalledAppFlow.from_client_secrets_file(
        client_secret_file,
        scopes=scopes,
        redirect_uri="urn:ietf:wg:oauth:2.0:oob"
    )
    auth_url, _ = flow.authorization_url(prompt="consent")
    return flow, auth_url


def _get_token(flow, code: str):
    flow.fetch_token(code=code)
    return flow.credentials


def load_service_account_credentials_from_file(path: str, scopes: list):
    credentials = service_account.Credentials.from_service_account_file(path, scopes=scopes)
    if not credentials.valid:
        request = google.auth.transport.requests.Request()
        try:
            credentials.refresh(request)
        except google.auth.exceptions.RefreshError as exc:
            # Credentials could be expired or revoked.
            LOGGER.error("Error refreshing credentials: {}".format(str(exc)))
            return None

    return credentials
