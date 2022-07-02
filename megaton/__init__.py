import sys
from pkg_resources import get_distribution


def get_ver(package):
    return get_distribution(package).version


def ipython_shell():
    """Same as `get_ipython` but returns `False` if not in IPython"""
    try:
        return get_ipython()
    except NameError:
        return False


def in_ipython():
    """Check if code is running in some kind of IPython environment"""
    return bool(ipython_shell())


def in_colab():
    """Check if the code is running in Google Colaboratory"""
    return 'google.colab' in sys.modules


def in_jupyter():
    """Check if the code is running in a jupyter notebook"""
    if not in_ipython():
        return False
    return ipython_shell().__class__.__name__ == 'ZMQInteractiveShell'


IN_JUPYTER, IN_COLAB = in_jupyter(), in_colab()
__version__ = get_ver('megaton')

if IN_COLAB:
    # enable data table
    from google.colab import data_table
    data_table.enable_dataframe_formatter()
    # mount google drive
    from . import gdrive
    json_path = gdrive.link_nbs()

print("__init__")
