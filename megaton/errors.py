"""Common error types used across the library."""


class Error(Exception):
    """Base class for other exceptions"""
    pass


class ApiDisabled(Error):
    """The API is not enabled for the GCP project being used"""

    def __init__(self, message=None, api=None):
        self.message = message or f"The API {api} is not enabled for the GCP project."
        self.api = api


class BadCredentialFormat(Error):
    """Credentials given are not an instance of google.oauth2.credentials.Credentials"""

    def __init__(self, message=None):
        self.message = message or "The given credentials are not in the expected format."


class BadCredentialScope(Error):
    """Credentials have insufficient scopes to use API"""

    def __init__(self, message=None, scopes=None):
        self.message = message or "The given credentials have insufficient scopes to use the API."
        self.scopes = scopes

    def __str__(self):
        return f'{self.message} Required scopes are: {self.scopes}'


class BadPermission(Error):
    """Permission is insufficient to perform API task"""

    def __init__(self, message=None):
        self.message = message or "Permission is insufficient to perform the API task."


class BadRequest(Error):
    """"Request given has errors"""

    def __init__(self, message=None):
        self.message = message or "There is an error in the request."


class BadUrlFormat(Error):
    """URL given is not in valid format"""

    def __init__(self, message=None):
        self.message = message or "The URL given is not in valid format."


class UrlNotFound(Error):
    """URL given is not found"""

    def __init__(self, message=None):
        self.message = message or "The URL requested is not found."


class NoDataReturned(Error):
    """No data was returned from API."""
    pass


class PartialDataReturned(Error):
    """Only partial data was returned from API.
    clientId is not officially supported by Google. Using this dimension in an
    Analytics report may thus result in unexpected & unexplainable behavior
    (such as restricting the report to exactly 10,000 or 10,001 rows)."""

    def __init__(self, message="Partial data returned from API: This occurs when "
                               "clientId is included in the request's dimensions."):
        self.message = message
        super().__init__(self.message)


class SheetNotFound(Error):
    """Sheet specified is not found in the workbook"""

    def __init__(self, message=None):
        self.message = message or "The sheet requested is not found."
