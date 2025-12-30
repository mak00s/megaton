import os


def install():
    """Install packages for GA4."""
    os.system("pip install -U -q google-analytics-admin")
    os.system("pip install -U -q google-analytics-data")
    # update packages
    # os.system("pip install -U -q google-api-core")
    # os.system("pip install -U -q google-auth")
