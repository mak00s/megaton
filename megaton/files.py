"""Functions for saving and downloading files
"""

import glob
import os
import sys

import pandas as pd


def in_colab():
    """Check if the code is running in Google Colaboratory"""
    return 'google.colab' in sys.modules


IN_COLAB = in_colab()

if IN_COLAB:
    from google.colab import files


def append_suffix_to_filename(filename: str, suffix: str, ext: str = '.csv'):
    """Add a suffix to a filename.
    .csv is added if extension is not included in the filename provided.
    """
    name, current_ext = os.path.splitext(filename)
    new_ext = current_ext if current_ext else ext
    return f"{name}{suffix}{new_ext}"


def load_df(filename: str):
    """Load CSV to a DataFrame
    """
    df = pd.concat(map(pd.read_csv, glob.iglob(filename, recursive=True)))
    return df


def save_df(df: pd.DataFrame, filename: str, mode: str = 'w', include_header: bool = True):
    """Save a DataFrame as CSV
    """
    if mode == 'a':
        include_header = False

    df.to_csv(filename, mode=mode, index=False, header=include_header)


def download_file(filename: str):
    """Download a file from Google Colaboratory
    """
    if IN_COLAB:
        files.download(filename)
