import requests
from requests.auth import HTTPBasicAuth
import json
import time

from base_intermediator import base_intermediator

class planet_mm(base_intermediator):
    def __init__(self, auth_key, aoi, mosaic_name):
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
        self.change_AOI(aoi)
        self.change_mosaic(mosaic_name)

    def authenticate(self):
        self.auth = HTTPBasicAuth(self.auth_key, '')
        self.session = requests.Session()
        self.session.auth = (self.auth_key, '')

    def change_AOI(self, geometry):
        self.order_params['products'][0]['geometry'] = geometry

    def change_AOI_from_file(self, file_path):
        file = open(file_path)
        geojson = json.load(file)
        self.change_AOI(geojson['features'][0]['geometry'])

    def change_mosaic(self, mosaic_name):
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
