import os
import sys
from IPython.display import clear_output
from pkg_resources import get_distribution
from time import sleep


__all__ = ['start']
# __version__ = get_distribution('megaton').version


try:
    # check if packages for GA4 are installed
    from google.analytics.data import BetaAnalyticsDataClient
    from google.analytics.admin import AnalyticsAdminServiceClient
except ModuleNotFoundError:
    clear_output()
    print("Installing packages for GA4...")
    from .install import ga4

    clear_output()
    # print("Runtime is now restarting...")
    # print("You can ignore the error message [Your session crashed for an unknown reason.]")
    print("もう一度このセルを実行してください。")
    sleep(0.5)
    os._exit(0)  # restart


# if the code is running in Google Colaboratory
if 'google.colab' in sys.modules:
    # enable data table
    from google.colab import data_table
    data_table.enable_dataframe_formatter()

    # mount google drive
    from . import gdrive
    json_path = gdrive.link_nbs()
