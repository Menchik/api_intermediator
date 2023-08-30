import requests
from requests.auth import HTTPBasicAuth
import json
import time
import shapefile

from base_intermediator import base_intermediator

from multiprocessing.pool import ThreadPool
from multiprocessing import cpu_count
import os

from utils import download_from_url

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

    def set_mosaic(self, mosaic_name):
        self.order_params['products'][0]['mosaic_name'] = mosaic_name

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

    def download_files(self, num_threads, result):

        #### num_threads = 0 for max threads possible

        cwd = os.getcwd()
 
        result = result.json()
        links = result['_links']['results']

        if not os.path.exists('./downloads'):
            os.mkdir('./downloads')

        os.chdir('./downloads')

        if(num_threads == 0):
            num_threads = cpu_count()-1
        dwnl_result = ThreadPool(num_threads).imap_unordered(download_from_url, links[-1])

        os.chdir(cwd)
        
        return dwnl_result