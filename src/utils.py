import os
from osgeo_utils import gdal_merge
import requests

def download_from_url(url):
    file_name_start_pos = url['name'].rfind("/") + 1
    file_name = url['name'][file_name_start_pos:]
    try:
        r = requests.get(url['location'], stream=True)
        if r.status_code == requests.codes.ok:
            with open(file_name, 'wb') as f:
                for data in r:
                    f.write(data)
    except Exception as e:
        print('Exception when downloading:', e)

def merge_tifs(tif_list):
    merge_params = ['', '-o', 'merged.tif']
    tif_list = merge_params + tif_list
    gdal_merge.gdal_merge(tif_list)

def merge_tifs_in_folder(folder_path):
    os.chdir(folder_path)
    tif_list = os.listdir('.')
    merge_tifs(tif_list)