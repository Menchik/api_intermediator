import os
from osgeo import gdal
from osgeo_utils import gdal_merge
import requests

def download_from_url(url):
    file_name_start_pos = url['name'].rfind("/") + 1
    file_name = url['name'][file_name_start_pos:]
    file_name = "downloads/" + file_name

    #Caso o arquivo já exista na pasta não baixa ele de novo
    if os.path.isfile(file_name):
        return
    
    try:
        r = requests.get(url['location'], stream=True)
        if r.status_code == requests.codes.ok:
            with open(file_name, 'wb') as f:
                for data in r:
                    f.write(data)
    except Exception as e:
        print('Exception when downloading:', e)

def merge_tifs(tif_list):
    gdal.UseExceptions()
    merge_params = ['', '-o', 'merged.tif']
    tif_list = merge_params + tif_list
    gdal_merge.gdal_merge(tif_list)

def merge_tifs_in_folder(folder_path):
    cwd = os.getcwd()
    os.chdir(folder_path)
    tif_list = os.listdir('.')
    try:
        merge_tifs(tif_list)
    except:
        pass
    os.chdir(cwd)


"""
Utility functions used by example notebooks
"""
from typing import Any, Optional, Tuple

import matplotlib.pyplot as plt
import numpy as np


def plot_image(
    image: np.ndarray,
    factor: float = 1.0,
    clip_range: Optional[Tuple[float, float]] = None,
    **kwargs: Any
) -> None:
    """Utility function for plotting RGB images."""
    fig, ax = plt.subplots(nrows=1, ncols=1, figsize=(15, 15))
    if clip_range is not None:
        ax.imshow(np.clip(image * factor, *clip_range), **kwargs)
    else:
        ax.imshow(image * factor, **kwargs)
    ax.set_xticks([])
    ax.set_yticks([])
