"""
Functions for Google API
"""

import json
import logging
import os
import time

from googleapiclient import errors
from googleapiclient.discovery import build, DISCOVERY_URI

_REQUIRED_CONFIG_KEYS = frozenset(("auth_uri", "token_uri", "client_id"))


class GoogleApi(object):
    """Google API helper object"""

    def __init__(self, api="oauth2", version="v2", scopes=None, *args,
                 **kwargs):
        """constructor"""
        if scopes is None:
            scopes = ['https://www.googleapis.com/auth/analytics.readonly']
        self.api = api
        self.api_version = version
        self.scopes = scopes
        self.credentials = kwargs.get('credentials')
        self._service = None
        self.discovery_url = kwargs.get('discovery_url', DISCOVERY_URI)
        self.retries = kwargs.get('retries', 3)
        self.credential_cache_file = kwargs.get('credential_cache_file', "creden-cache.json")
        self.cache_dir = kwargs.get('cache_dir', ".")
        self.log = logging.getLogger("__name__")

    @property
    def service(self):
        """get or create a api service"""
        if self._service is None:
            # self.log.debug(f"Creating a service for {self.api} API")
            self._service = build(self.api,
                                  self.api_version,
                                  credentials=self.credentials,
                                  # cache=program_memory_cache,
                                  discoveryServiceUrl=self.discovery_url)
        return self._service

    def auth(self, file: str):
        if not os.path.isdir(self.cache_dir):
            os.makedirs(self.cache_dir)
        cache_path = os.path.join(self.cache_dir, self.credential_cache_file)

        credentials = get_credentials(file, self.scopes, cache_path)

        self.credentials = credentials
        self._service = None
        return self

    def retry(self, service_method, retry_count=0):
        """
        retry a google api call and check for rate limits
        """
        try:
            return service_method.execute(num_retries=retry_count)
        except errors.HttpError as e:
            code = e.resp.get('code')
            reason = ''
            message = ''
            try:
                data = json.loads(e.content.decode('utf-8'))
                code = data['error']["code"]
                message = data['error']['message']
                reason = data['error']['errors'][0]['reason']
            except:  # noqa
                pass

            if code == 403 and "rate limit exceeded" in message.lower():
                self.log.debug("rate limit reached, sleeping for %s seconds", 2 ** retry_count)
                time.sleep(2 ** retry_count)
                return self.retry(service_method, retry_count + 1)
            elif code == 403 and ("accessNotConfigured" in reason or 'disabled' in message):
                self.log.debug(message)
                raise
            else:
                self.log.debug(f"got HttpError (content={data}")
                raise
        except BrokenPipeError:
            self.log.info("BrokenPipeError occurred but attempting to retry")
            return self.retry(service_method, retry_count + 1)
        except KeyboardInterrupt:
            raise
        except:  # noqa
            self.log.exception("Failed to execute api method")
            raise

    def __getattr__(self, name):
        """ get attribute or service wrapper
        :param name: attribute / service name
        :return:
        """
        return getattr(MethodHelper(self, self.service), name)

    @classmethod
    def ga_reporting(cls, version="v4"):
        """Google Analytics Reporting API v4"""
        return GoogleApi("analyticsreporting", version, ["https://www.googleapis.com/auth/analytics.readonly"])

    @classmethod
    def ga_management(cls, version="v3"):
        """Google Analytics Management API v3"""
        return GoogleApi("analytics", version, ["https://www.googleapis.com/auth/analytics.readonly"])


class MethodHelper(object):
    """ helper to streamline api calls"""

    def __init__(self, google_api, service, name=None, path=None):
        """
        create a method helper
        :param google_api GoogleApi instance of api
        :param service Google API service (GoogleApi.service) or method of it
        :param name method name
        :param path API path i.e. for compute: instances.list
        """
        self.google_api = google_api
        self.service = service
        self.name = name
        self.path = path if path is not None else []
        if name is not None:
            self.path.append(name)
        # print("constructor %s", name)

    def execute(self, *args, **kwargs):
        """execute service api"""
        # self.log.info("execute %s", self.name)
        return self.google_api.retry(self.service)

    def call(self, *args, **kwargs):
        """
        wrapper for service methods
        this wraps an GoogleApi.service call so the next level can also use helpers
        i.e. for compute v1 api GoogleApi.service.instances() can be used as Google.instances()
        and will return a MethodHelper instance
        """
        # self.log.info("call %s", self.name)
        return MethodHelper(self.google_api, getattr(self.service, self.name)(*args, **kwargs))

    def __getattr__(self, name):
        """ get service method """
        # self.log.info("getattr %s", name)
        if not hasattr(self.service, name):
            err_msg = u"API method {} unknown on {} {}".format(u".".join(self.path + [name]),
                                                               self.google_api.api,
                                                               self.google_api.api_version)
            raise RuntimeError(err_msg)
        return MethodHelper(self.google_api, self.service, name, self.path).call
