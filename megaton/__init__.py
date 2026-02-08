import os
import sys
from IPython.display import clear_output


__all__ = ['mount_google_drive']


def _is_colab() -> bool:
    return 'google.colab' in sys.modules


def _auto_install_enabled() -> bool:
    env_value = os.environ.get("MEGATON_AUTO_INSTALL")
    if env_value == "1":
        return True
    if env_value == "0":
        return False
    return _is_colab()


def _print_install_help():
    print(
        "Megaton requires GA4 packages. Install with:\n"
        "  pip install -U -q google-analytics-admin google-analytics-data\n"
        "  pip install -U -q google-cloud-bigquery-datatransfer\n"
        "Or set MEGATON_AUTO_INSTALL=1 in Colab."
    )


try:
    # check if packages for GA4 are installed
    from google.analytics.data import BetaAnalyticsDataClient
    from google.analytics.admin import AnalyticsAdminServiceClient
except ModuleNotFoundError:
    if _auto_install_enabled():
        clear_output()
        print("Installing packages for GA4...")
        from .install import install_ga4, install_bigquery

        install_ga4.install()
        install_bigquery.install()
        clear_output()
    else:
        _print_install_help()
        raise

IS_COLAB = _is_colab()

if IS_COLAB:
    from google.colab import data_table
    data_table.enable_dataframe_formatter()
    data_table._DEFAULT_FORMATTERS[float] = lambda x: f"{x:.3f}"


def mount_google_drive():
    '''Mount Google Drive when running in Google Colab.'''
    if not IS_COLAB:
        print("Google Drive mounting is only available in Google Colab.")
        return None
    from . import gdrive
    return gdrive.link_nbs()
