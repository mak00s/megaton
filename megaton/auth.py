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


def _is_service_account(json_text: str):
    """Return true if the provided text is a JSON service credentials file."""
    try:
        key_obj = json.loads(json_text)
    except json.JSONDecodeError:
        return False
    if not key_obj or key_obj.get('type', '') != 'service_account':
        return False
    return True


def _is_service_account_json(json_path: str):
    """Return true if the provided JSON file is for a service account."""
    with open(json_path, 'r') as f:
        return _is_service_account(f.read())


def get_credential_type(client_config: dict):
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


def get_credential_type_from_file(json_path: str):
    """Gets a client type from a Google client secrets file.

        Args:
            json_path (str): The path to the client secrets .json file.

        Returns:
            client_type [str]: The client type, either ``'service_account'`` or ``'web'`` or ``'installed'``
        """
    with open(json_path, "r") as json_path:
        client_config = json.load(json_path)

    return get_credential_type(client_config)


def get_json_files_from_dir(json_dir: str):
    """Gets a list of valid credentials json files from a directory recursively"""
    json_files = defaultdict(lambda: {})
    for root, dirs, files in os.walk(json_dir):
        for file in files:
            if file.endswith('.json'):
                client_type = get_credential_type_from_file(os.path.join(root, file))
                if client_type == 'service_account':
                    json_files['Service Account'][file] = os.path.join(root, file)
                elif client_type in ['installed', 'web']:
                    json_files['OAuth'][file] = os.path.join(root, file)
    return json_files


def get_cache_path(json_path: str):
    """Gets the path to the Google user credentials based on the provided source file
    """
    dir_path = os.path.join(os.path.expanduser("~"), ".config")
    os.makedirs(dir_path, exist_ok=True)
    base_name = os.path.splitext(os.path.basename(json_path))[0]
    return os.path.join(dir_path, f"cache_{base_name}.json")


def save_credentials(file_path: str, credentials: Credentials):
    """Save Credentials to cache file
    """
    cache_path = get_cache_path(file_path)
    with open(cache_path, 'w') as w:
        LOGGER.debug(f"saving credentials to {cache_path}")
        w.write(credentials.to_json())
    return credentials


def load_credentials(file_path: str, scopes: list):
    """Load Credentials from cache file
    """
    cache_path = get_cache_path(file_path)
    if os.path.isfile(cache_path):
        LOGGER.debug(f"loading credentials from {cache_path}")
        return Credentials.from_authorized_user_file(cache_path, scopes=scopes)


def delete_credentials(cache_file: str = "creden-cache.json"):
    """Delete Credentials cache file
    """
    if os.path.isfile(cache_file):
        LOGGER.debug(f"deleting cache file {cache_file}")
        os.remove(cache_file)


def get_oauth_redirect(client_secret_file: str, scopes: list):
    """Run OAuth2 Flow"""
    flow = InstalledAppFlow.from_client_secrets_file(
        client_secret_file,
        scopes=scopes,
        redirect_uri="urn:ietf:wg:oauth:2.0:oob"
    )
    auth_url, _ = flow.authorization_url(prompt="consent")
    return flow, auth_url


def get_token(flow, code: str):
    flow.fetch_token(code=code)
    return flow.credentials


def load_service_account_credentials_from_file(path: str, scopes: list):
    """Gets service account credentials from JSON file at ``path``.

    :param path: Path to credentials JSON file.
    :param scopes: A list of scopes to use when authenticating to Google APIs.
    :return: google.oauth2.service_account.Credentials
    """
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
