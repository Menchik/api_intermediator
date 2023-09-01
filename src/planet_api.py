import requests
from requests.auth import HTTPBasicAuth
import json
import time
import shapefile

from .base_intermediator import base_intermediator
from .utils import download_from_url

from multiprocessing.pool import ThreadPool
from multiprocessing import cpu_count
import os


class planet_mm(base_intermediator):
    def __init__(self, auth_key):
        base_intermediator.__init__(self, auth_key)
        self.ORDERS_API_URL = 'https://api.planet.com/compute/ops/orders/v2'
        self.order_params = {
                "name": "basemap order with geometry",
                "source_type": "basemaps",
                "order_type": "partial",
                "products": [
                    {
                        "mosaic_name": "placeholder",
                        "geometry": {}
                    }
                ]
            }
        self.mosaic_list = None
        self.update_mosaic()

    def authenticate(self):
        self.auth = HTTPBasicAuth(self.auth_key, '')
        self.session = requests.Session()
        self.session.auth = (self.auth_key, '')

    def set_AOI_geometry(self, geometry):
        self.order_params['products'][0]['geometry'] = geometry

    def set_AOI_geojson(self, file_path):
        file = open(file_path)
        geojson = json.load(file)
        self.set_AOI_geometry(geojson['features'][0]['geometry'])

    def set_AOI_shapefile(self, file_path):
        # read the shapefile
        reader = shapefile.Reader(file_path)

        #Get geometry from shapefile
        geom = []
        for sr in reader.shapeRecords():
            geom.append(sr.shape.__geo_interface__)

        self.set_AOI_geometry(geom[0])

    def update_mosaic(self):
        print("-Listing mosaics")
        MOSAIC_LIST_URL = 'https://api.planet.com/basemaps/v1/mosaics'
        response = self.session.get(MOSAIC_LIST_URL, auth=self.auth)
        
        basemaps = response.raise_for_status()
        if response.status_code != 204:
            basemaps = json.loads(response.text)

        mosaic_list = []
        for mosaic_name in basemaps['mosaics']:
            self.mosaic_list.append(mosaic_name['name'])

        self.mosaic_list = mosaic_list

    def print_mosaic_list(self):
        for  i, mosaic_name in enumerate(self.mosaic_list):
            print(f"{i} : {mosaic_name}")

    def set_mosaic(self, mosaic_value):
        self.order_params['products'][0]['mosaic_name'] = self.mosaic_list[mosaic_value]
        print(f"-Sucess, mosaic set {mosaic_value} : \"{self.mosaic_list[mosaic_value]}\"")

    def place_order(self):
        print("-Placing order")
        response = self.session.post(self.ORDERS_API_URL, 
                            data=json.dumps(self.order_params), 
                            auth=self.auth, 
                            headers={'content-type': 'application/json'})
        order_id = response.json()['id']
        order_url = self.ORDERS_API_URL + '/' + order_id
        return order_url
    
    def poll_for_success(self, order_url):
        print("-Polling")
        state = ''
        end_states = ['success', 'failed', 'partial']
        while state not in end_states:
            r = self.session.get(order_url, auth=self.session.auth)
            response = r.json()
            state = response['state']
            print(state)
            if state in end_states:
                break
            time.sleep(10)
        return r
    
    def get_basemap(self):
        order_url = self.place_order()
        return self.poll_for_success(order_url)

    def download_files(self, num_threads, result, allFiles=False):

        #### num_threads = 0 for max threads possible

        cwd = os.getcwd()
 
        result = result.json()
        step = 4** (not allFiles)
        links = result['_links']['results'][0::step]

        if not os.path.exists('./downloads'):
            os.mkdir('./downloads')

        os.chdir('./downloads')

        if(num_threads == 0):
            num_threads = cpu_count()-1
        dwnl_result = ThreadPool(num_threads).imap_unordered(download_from_url, links[-1])

        os.chdir(cwd)
        
        return dwnl_result