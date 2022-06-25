import os
import sys
from pathlib import Path

from google.colab.drive import mount


def link_nbs():
    """ Create /nbs as a symlink to "Colab Notebooks".
    Also add /nbs to python path, so we can store libraries there
    Allow import A to get A.ipybn as well
    """
    if not Path("/content/drive").exists():
        mount("/content/drive")
    # create link if not already
    if not Path("/nbs").exists():
        os.symlink("/content/drive/My Drive/Colab Notebooks", "/nbs")
    # add path if not already
    if '/nbs' not in sys.path:
        sys.path.insert(5, '/nbs')  # before dist-packages
    return '/nbs'
