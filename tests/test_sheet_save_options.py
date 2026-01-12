import pandas as pd
from unittest.mock import MagicMock

from megaton.start import Megaton


def test_save_to_sheet_passes_options():
    app = Megaton()
    app._sheets = MagicMock()
    df = pd.DataFrame({"a": [2, 1]})

    app.save.to.sheet(
        "Sheet1",
        df,
        sort_by="a",
        sort_desc=False,
        auto_width=True,
        freeze_header=True,
    )

    app._sheets.save_sheet.assert_called_once_with(
        "Sheet1",
        df,
        sort_by="a",
        sort_desc=False,
        auto_width=True,
        freeze_header=True,
    )


def test_sheet_save_passes_options():
    app = Megaton()
    app._sheets = MagicMock()
    app.sheet._ensure_spreadsheet = MagicMock()
    app.sheet._ensure_sheet_selected = MagicMock(return_value="Sheet2")
    df = pd.DataFrame({"a": [1]})

    app.sheet.save(
        df,
        sort_by="a",
        auto_width=True,
        freeze_header=True,
    )

    app._sheets.save_sheet.assert_called_once_with(
        "Sheet2",
        df,
        sort_by="a",
        sort_desc=True,
        auto_width=True,
        freeze_header=True,
    )
