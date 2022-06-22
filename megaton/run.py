"""run megaton"""

from IPython.display import clear_output
import os
from time import sleep

from . import gdrive


# install packages for GA4
try:
    from google.analytics.data import BetaAnalyticsDataClient
except ModuleNotFoundError:
    print("Installing packages for GA4")
    from .install import ga4
    from .update import google_api_core

    clear_output()
    # print("Runtime is now restarting...")
    # print("You can ignore the error message [Your session crashed for an unknown reason.]")
    print("もう一度このセルを実行してください。")
    sleep(0.5)
    os._exit(0)  # restart
else:
    gdrive.link_nbs()
    print("準備ができました。")
