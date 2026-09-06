"""Notebook-facing Sheets namespaces, independent of session orchestration."""

from typing import Optional

import pandas as pd

from .._result import _extract_df


class Sheets:
    """Spreadsheet-level helpers (selection/creation)"""
    def __init__(self, parent):
        self.parent = parent

    def _ensure_spreadsheet(self):
        if not self.parent.gs or not self.parent.state.gs_url:
            raise ValueError(
                "Google Sheetsが開かれていません。\n"
                "先に mg.open.sheet(url) でスプレッドシートを開いてください。\n"
                "例: mg.open.sheet('https://docs.google.com/spreadsheets/d/...')"
            ) from None

    def select(self, name: str):
        self._ensure_spreadsheet()
        selected = self.parent.gs.sheet.select(name)
        if selected:
            self.parent.state.gs_sheet_name = name
        return selected

    def read(self, name: str) -> pd.DataFrame:
        """シートを選択してデータを DataFrame として返すショートカット。

        Args:
            name: worksheet name

        Returns:
            DataFrame of sheet data (empty DataFrame if no data)
        """
        self.select(name)
        data = self.parent.gs.sheet.data
        if isinstance(data, pd.DataFrame):
            return data
        return pd.DataFrame(data or [])

    def create(self, name: str):
        self._ensure_spreadsheet()
        self.parent.gs.sheet.create(name)
        self.parent.state.gs_sheet_name = name
        return name

    def duplicate(self, source_name: str, new_name: str, *, cell_update: Optional[dict] = None):
        self._ensure_spreadsheet()
        return self.parent._sheets.duplicate_sheet(
            self.parent.state.gs_url,
            source_name,
            new_name,
            cell_update=cell_update,
        )

    def delete(self, name: str):
        self._ensure_spreadsheet()
        if name not in self.parent.gs.sheets:
            raise ValueError(f"Sheet not found: {name}")
        self.parent.gs.sheet.delete(name)
        if self.parent.state.gs_sheet_name == name:
            self.parent.state.gs_sheet_name = None
        return True

class Sheet:
    """Notebook-facing current worksheet helpers"""
    def __init__(self, parent):
        self.parent = parent
        self.cell = self.Cell(self)
        self.range = self.Range(self)
        self.gridlines = self.Gridlines(self)
        self.tab = self.Tab(self)

    def _ensure_spreadsheet(self):
        if not self.parent.gs or not self.parent.state.gs_url:
            raise ValueError(
                "Google Sheetsが開かれていません。\n"
                "先に mg.open.sheet(url) でスプレッドシートを開いてください。\n"
                "例: mg.open.sheet('https://docs.google.com/spreadsheets/d/...')"
            ) from None

    def _current_sheet_name(self):
        name = self.parent.state.gs_sheet_name
        if not name and self.parent.gs:
            name = self.parent.gs.sheet.name
            if name:
                self.parent.state.gs_sheet_name = name
        return name

    def _ensure_sheet_selected(self):
        name = self._current_sheet_name()
        if not name:
            raise ValueError("No worksheet selected. Call mg.sheets.select(name) first.")
        return name

    def clear(self):
        self._ensure_spreadsheet()
        self._ensure_sheet_selected()
        return self.parent.gs.sheet.clear()

    def freeze(
        self,
        rows: Optional[int] = None,
        cols: Optional[int] = None,
        *,
        max_retries: Optional[int] = None,
        backoff_factor: Optional[float] = None,
    ):
        """Freeze rows and/or columns on the selected worksheet."""
        self._ensure_spreadsheet()
        self._ensure_sheet_selected()
        return self.parent.gs.sheet.freeze(
            rows=rows,
            cols=cols,
            max_retries=max_retries,
            backoff_factor=backoff_factor,
        )

    def resize(
        self,
        *,
        rows: Optional[int] = None,
        cols: Optional[int] = None,
        shrink: bool = False,
        max_retries: Optional[int] = None,
        backoff_factor: Optional[float] = None,
    ):
        """Resize the selected worksheet grid.

        Defaults to expand-only; pass ``shrink=True`` to allow reducing row
        or column counts.
        """
        self._ensure_spreadsheet()
        self._ensure_sheet_selected()
        return self.parent.gs.sheet.resize_dimensions(
            rows=rows,
            cols=cols,
            shrink=shrink,
            max_retries=max_retries,
            backoff_factor=backoff_factor,
        )

    @property
    def data(self):
        self._ensure_spreadsheet()
        self._ensure_sheet_selected()
        return self.parent.gs.sheet.data


    def _coerce_df(self, df):
        df = _extract_df(df)
        if df is None:
            df = self.parent.report.data
        if not isinstance(df, pd.DataFrame):
            raise TypeError(
                "df must be a pandas DataFrame (or omit df to use mg.report.data)."
            )
        return df

    def save(
        self,
        df: pd.DataFrame = None,
        *,
        sort_by=None,
        sort_desc: bool = True,
        start_row: int = 1,
        auto_width: bool = False,
        freeze_header: bool = False,
        max_retries: Optional[int] = None,
        backoff_factor: Optional[float] = None,
    ):
        df = self._coerce_df(df)
        self._ensure_spreadsheet()
        sheet_name = self._ensure_sheet_selected()
        return self.parent._sheets.save_sheet(
            sheet_name,
            df,
            sort_by=sort_by,
            sort_desc=sort_desc,
            start_row=start_row,
            auto_width=auto_width,
            freeze_header=freeze_header,
            max_retries=max_retries,
            backoff_factor=backoff_factor,
        )

    def append(
        self,
        df: pd.DataFrame = None,
        *,
        auto_width: bool = False,
        freeze_header: bool = False,
        max_retries: Optional[int] = None,
        backoff_factor: Optional[float] = None,
    ):
        df = self._coerce_df(df)
        self._ensure_spreadsheet()
        sheet_name = self._ensure_sheet_selected()
        return self.parent._sheets.append_sheet(
            sheet_name,
            df,
            auto_width=auto_width,
            freeze_header=freeze_header,
            max_retries=max_retries,
            backoff_factor=backoff_factor,
        )

    def upsert(
        self,
        df: pd.DataFrame = None,
        *,
        keys,
        columns=None,
        sort_by=None,
        auto_width: bool = False,
        freeze_header: bool = False,
        max_retries: Optional[int] = None,
        backoff_factor: Optional[float] = None,
    ):
        df = self._coerce_df(df)
        self._ensure_spreadsheet()
        sheet_url = self.parent.state.gs_url
        sheet_name = self._ensure_sheet_selected()
        return self.parent._sheets.upsert_df(
            sheet_url,
            sheet_name,
            df,
            keys=keys,
            columns=columns,
            sort_by=sort_by,
            create_if_missing=True,
            auto_width=auto_width,
            freeze_header=freeze_header,
            max_retries=max_retries,
            backoff_factor=backoff_factor,
        )

    class Cell:
        def __init__(self, parent):
            self.parent = parent

        def set(self, cell: str, value):
            app = self.parent.parent
            app.sheet._ensure_spreadsheet()
            sheet_name = app.sheet._ensure_sheet_selected()
            return app._sheets.update_cells(
                app.state.gs_url,
                sheet_name,
                {cell: value},
            )

        def get(self, cell: str):
            """選択中シートのセル値を A1 表記で読む（例: ``mg.sheet.cell.get("A1")``）。"""
            app = self.parent.parent
            app.sheet._ensure_spreadsheet()
            app.sheet._ensure_sheet_selected()
            return app.gs.sheet.cell.select(cell)

    class Range:
        def __init__(self, parent):
            self.parent = parent

        def set(self, a1_range: str, values):
            app = self.parent.parent
            app.sheet._ensure_spreadsheet()
            sheet_name = app.sheet._ensure_sheet_selected()
            return app._sheets.update_range(
                app.state.gs_url,
                sheet_name,
                a1_range,
                values,
            )

    class Gridlines:
        def __init__(self, parent):
            self.parent = parent

        def hide(
            self,
            *,
            max_retries: Optional[int] = None,
            backoff_factor: Optional[float] = None,
        ):
            app = self.parent.parent
            app.sheet._ensure_spreadsheet()
            app.sheet._ensure_sheet_selected()
            return app.gs.sheet.set_gridlines(
                False,
                max_retries=max_retries,
                backoff_factor=backoff_factor,
            )

        def show(
            self,
            *,
            max_retries: Optional[int] = None,
            backoff_factor: Optional[float] = None,
        ):
            app = self.parent.parent
            app.sheet._ensure_spreadsheet()
            app.sheet._ensure_sheet_selected()
            return app.gs.sheet.set_gridlines(
                True,
                max_retries=max_retries,
                backoff_factor=backoff_factor,
            )

    class Tab:
        def __init__(self, parent):
            self.parent = parent

        def color(
            self,
            color,
            *,
            max_retries: Optional[int] = None,
            backoff_factor: Optional[float] = None,
        ):
            app = self.parent.parent
            app.sheet._ensure_spreadsheet()
            app.sheet._ensure_sheet_selected()
            return app.gs.sheet.set_tab_color(
                color,
                max_retries=max_retries,
                backoff_factor=backoff_factor,
            )
