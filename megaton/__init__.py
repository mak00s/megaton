"""Megaton package.

Keep import-time side effects minimal. Notebook/Colab conveniences and optional
dependency handling should be triggered at use-time (e.g. Megaton() init),
not on `import megaton`.
"""

from __future__ import annotations

import sys

__all__ = ["mount_google_drive"]


def _is_colab() -> bool:
    return "google.colab" in sys.modules


IS_COLAB = _is_colab()

if IS_COLAB:
    # Best-effort: never break imports for a UX tweak.
    try:
        from google.colab import data_table

        data_table.enable_dataframe_formatter()
        data_table._DEFAULT_FORMATTERS[float] = lambda x: f"{x:.3f}"
    except Exception:  # pragma: no cover
        pass


def mount_google_drive():
    """Mount Google Drive when running in Google Colab."""
    if not IS_COLAB:
        print("Google Drive mounting is only available in Google Colab.")
        return None
    from . import gdrive

    return gdrive.link_nbs()

