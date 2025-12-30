import sys
from IPython.display import clear_output


__all__ = ['start', 'mount_google_drive']


try:
    # check if packages for GA4 are installed
    from google.analytics.data import BetaAnalyticsDataClient
    from google.analytics.admin import AnalyticsAdminServiceClient
except ModuleNotFoundError:
    clear_output()
    print("Installing packages for GA4...")
    from .install import install_ga4
    from .install import install_bigquery

    clear_output()

IS_COLAB = 'google.colab' in sys.modules

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
