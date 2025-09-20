"""Utilities for Google Search Console"""

import logging
from typing import Optional

from google.oauth2.credentials import Credentials
from google.oauth2 import service_account

from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

from . import errors

LOGGER = logging.getLogger(__name__)


class MegatonSC(object):
    """Google Search Console client"""

    required_scopes = ['https://www.googleapis.com/auth/webmasters.readonly']

    def __init__(self, credentials: Credentials, site_url: Optional[str] = None):
        self.credentials = credentials
        self._client = None
        self.site_url = None
        self._authorize()
        if site_url:
            self.set_site(site_url)

    def _authorize(self):
        if not isinstance(self.credentials, (Credentials, service_account.Credentials)):
            self.credentials = None
            raise errors.BadCredentialFormat
        elif self.credentials.scopes:
            if not set(self.required_scopes) <= set(self.credentials.scopes):
                self.credentials = None
                raise errors.BadCredentialScope(self.required_scopes)
        try:
            self._client = build('searchconsole', 'v1', credentials=self.credentials)
        except HttpError as exc:
            LOGGER.error('Search Console API error: %s', exc)
            raise

    def set_site(self, site_url: str):
        self.site_url = site_url

    @property
    def client(self):
        return self._client

