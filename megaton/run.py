"""run megaton"""

from IPython.display import clear_output
import os
from time import sleep

from . import auth, gdrive


try:
    # check if packages for GA4 are installed
    from google.analytics.data import BetaAnalyticsDataClient
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

# mount google drive
gdrive.link_nbs()

json_files = auth.get_client_secrets_from_dir('/nbs')

print("準備ができました。")
