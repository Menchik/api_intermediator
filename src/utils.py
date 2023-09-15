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