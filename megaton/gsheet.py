"""
Functions for Google Sheets
"""

from typing import Optional, Union
import logging
import pandas as pd

from google.oauth2.credentials import Credentials
from google.oauth2 import service_account
from google.auth.exceptions import RefreshError
from gspread_dataframe import set_with_dataframe
import gspread

from . import errors

LOGGER = logging.getLogger(__name__)


class MegatonGS(object):
    """Google Sheets client
    """
    required_scopes = [
        'https://www.googleapis.com/auth/spreadsheets',
        'https://www.googleapis.com/auth/drive',
    ]

    def __init__(self, credentials: Credentials, url: Optional[str] = None):
        """constructor"""
        self.credentials = credentials
        self._client: gspread.client.Client = None
        self._driver: gspread.spreadsheet.Spreadsheet = None
        self.sheet = self.Sheet(self)

        self._authorize()
        if url:
            self.open(url)

    def _authorize(self):
        """Validate credentials given and build client"""
        if not isinstance(self.credentials, (Credentials, service_account.Credentials)):
            self.credentials = None
            raise errors.BadCredentialFormat
        elif self.credentials.scopes:
            if not set(self.required_scopes) <= set(self.credentials.scopes):
                self.credentials = None
                raise errors.BadCredentialScope(self.required_scopes)
        self._client = gspread.authorize(self.credentials)

    @property
    def sheets(self):
        return [s.title for s in self._driver.worksheets()] if self._driver else []

    @property
    def title(self):
        return self._driver.title if self._driver else None

    @property
    def url(self):
        return self._driver.url if self._driver else None

    def open(self, url: str, sheet: str = None):
        """Get or create an api service

        Args:
            url (str): URL of the Google Sheets to open.
            sheet (str): Sheet name to open. optional

        Returns:
            title (str): the title of the Google Sheets opened
        """

        try:
            self._driver = self._client.open_by_url(url)
        except gspread.exceptions.NoValidUrlKeyFound:
            raise errors.BadUrlFormat

        try:
            title = self._driver.title
        except RefreshError:
            raise errors.BadCredentialScope
        except gspread.exceptions.APIError as e:
            if 'disabled' in str(e):
                raise errors.ApiDisabled
            elif 'PERMISSION_DENIED' in str(e):
                raise errors.BadPermission
            elif 'NOT_FOUND' in str(e):
                raise errors.UrlNotFound
            else:
                raise

        if sheet:
            self.sheet.select(sheet)

        return title

    class Sheet(object):
        def __init__(self, parent):
            """constructor"""
            self.parent = parent
            self._driver: gspread.worksheet.Worksheet = None
            self.cell = self.Cell(self)

        def _refresh(self):
            """Rebuild the Gspread client"""
            # self._driver = self.parent._driver.worksheet(self.name)
            self.select(self.name)

        def clear(self):
            """Blank all the cells on the sheet"""
            self._driver.clear()

        def create(self, name: str):
            if not self.parent._client:
                LOGGER.warn("Open URL first.")
                return

        def select(self, name: str):
            if not self.parent._client:
                LOGGER.error("Open URL first.")
                return
            try:
                self._driver = self.parent._driver.worksheet(name)
            except gspread.exceptions.WorksheetNotFound:
                raise errors.SheetNotFound
            except gspread.exceptions.APIError as e:
                if 'disabled' in str(e):
                    raise errors.ApiDisabled
                elif 'PERMISSION_DENIED' in str(e):
                    raise errors.BadPermission
            return self.name

        @property
        def id(self):
            """Sheet ID"""
            return self._driver.id if self._driver else None

        @property
        def name(self):
            """Sheet Name"""
            return self._driver.title if self._driver else None

        @property
        def last_row(self):
            """looks for the last row based on values appearing in all columns
            """
            cols = self._driver.range(1, 1, self._driver.row_count, self._driver.col_count)
            last = [cell.row for cell in cols if cell.value]
            return max(last) if last else 0

        @property
        def next_available_row(self):
            """looks for the first empty row based on values appearing in all columns
            """
            return self.last_row + 1

        @property
        def data(self):
            """Returns a list of dictionaries, all of them having the contents of
                    the spreadsheet with the head row as keys and each of these
                    dictionaries holding the contents of subsequent rows of cells as
                    values.
            """
            if not self._driver:
                LOGGER.error("Please select a sheet first.")
                return
            data = self._driver.get_all_records()
            return data

        def auto_resize(self, cols: list):
            """Auto resize columns to fit text"""
            sheet_id = self.id
            _requests = []
            for i in cols:
                dim = {
                    "autoResizeDimensions": {
                        "dimensions": {
                            "sheetId": sheet_id,
                            "dimension": "COLUMNS",
                            "startIndex": i - 1,
                            "endIndex": i
                        }
                    }
                }
                _requests.append(dim)
            self.parent._driver.batch_update({'requests': _requests})

        def resize(self, col: int, width: int):
            """Resize columns"""
            sheet_id = self.id
            _requests = []
            for i in [col]:
                dim = {
                    "updateDimensionProperties": {
                        "range": {
                            "sheetId": sheet_id,
                            "dimension": "COLUMNS",
                            "startIndex": i - 1,
                            "endIndex": i
                        },
                        "properties": {
                            "pixelSize": width
                        },
                        "fields": "pixelSize"
                    }
                }
                _requests.append(dim)
            self.parent._driver.batch_update({'requests': _requests})

        def freeze(self, rows: Optional[int] = None, cols: Optional[int] = None):
            """Freeze rows and/or columns on the worksheet"""
            self._driver.freeze(rows=rows, cols=cols)

        def save_data(self, df: pd.DataFrame, mode: str = 'a', row: int = 1, include_index: bool = False):
            """Save the dataframe to the sheet"""
            if not len(df):
                LOGGER.info("no data to write.")
                return
            elif not self._driver:
                LOGGER.warn("Please select a sheet first.")
                return
            elif mode == 'w':
                try:
                    self.clear()
                except gspread.exceptions.APIError as e:
                    if 'disabled' in str(e):
                        raise errors.ApiDisabled
                    elif 'PERMISSION_DENIED' in str(e):
                        raise errors.BadPermission

                set_with_dataframe(
                    self._driver,
                    df,
                    include_index=include_index,
                    include_column_header=True,
                    row=row,
                    resize=True
                )
                return True
            elif mode == 'a':
                next_row = self.next_available_row
                current_row = self._driver.row_count
                new_rows = df.shape[0]
                if current_row < next_row + new_rows - 1:
                    LOGGER.debug(f"adding {next_row + new_rows - current_row - 1} rows")
                    self._driver.add_rows(next_row + new_rows - current_row)
                    self._refresh()

                set_with_dataframe(
                    self._driver,
                    df,
                    include_index=include_index,
                    include_column_header=False,
                    row=next_row,
                    resize=False
                )
                return True

        def overwrite_data(self, df: pd.DataFrame, include_index: bool = False):
            """Clear the sheet and save the dataframe"""
            return self.save_data(df, mode='w', include_index=include_index)

        class Cell(object):
            def __init__(self, parent):
                self.parent = parent
                self.address: str = ''
                # self.data = None

            @property
            def data(self):
                if self.address:
                    return self.parent._driver.acell(self.address).value

            def select(self, row: Union[int, str], col: Optional[int] = None):
                if not self.parent._driver:
                    LOGGER.error("Please select a sheet first.")
                    return

                self.address = gspread.utils.rowcol_to_a1(row, col) if col else row
                return self.data
