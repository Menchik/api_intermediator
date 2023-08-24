import os
from osgeo_utils import gdal_merge

def merge_tifs(tif_list):
    tif_list.insert(0, '')
    tif_list.insert(1, '-o')
    gdal_merge.gdal_merge(tif_list)

def merge_tifs_in_folder(folder_path):
    os.chdir(folder_path)
    tif_list = os.listdir('.')
    merge_tifs(tif_list)