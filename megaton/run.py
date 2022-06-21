"""run megaton"""

from IPython.display import clear_output
from time import sleep

from kora import os

# install packages for GA4
try:
    from google.analytics.data import BetaAnalyticsDataClient
except ModuleNotFoundError:
    print("Installing packages for GA4")
    from megaton.install import ga4
    from megaton.update import google_api_core

    clear_output()
    # print("Runtime is now restarting...")
    # print("You can ignore the error message [Your session crashed for an unknown reason.]")
    print("もう一度このセルを実行してください。")
    sleep(0.5)
    os._exit(0)  # restart
else:
    print("準備ができました。")
