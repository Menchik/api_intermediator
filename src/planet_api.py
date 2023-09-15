import requests
from requests.auth import HTTPBasicAuth
import json
import time
import shapefile

from .base_intermediator import base_intermediator

from multiprocessing.pool import ThreadPool
from multiprocessing import cpu_count
import os

from .utils import download_from_url

from shapely import wkt, to_geojson
from shapely.ops import linemerge, unary_union, polygonize
from shapely.geometry import shape

ERROR_TOO_MANY_QUADS = '{"field":null,"general":[{"message":"Unable to accept order: geometry for mosaic planet_medres_normalized_analytic_2015-12_2016-05_mosaic intersects'


class planet_mm(base_intermediator):
    def __init__(self, auth_key):
        base_intermediator.__init__(self, auth_key)
        self.mosaic_list = None
        self.update_mosaic()

    def authenticate(self):
        self.auth = HTTPBasicAuth(self.auth_key, '')
        self.session = requests.Session()
        self.session.auth = (self.auth_key, '')

    def set_AOI_geometry(self, geometry):
        self.geometry = geometry

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

    def place_order(self, geometry):
        print("-Placing order")

        ORDERS_API_URL = 'https://api.planet.com/compute/ops/orders/v2'
        
        self.order_params = {
            "name": "basemap order with geometry",
            "source_type": "basemaps",
            "order_type": "partial",
            "products": [
                {
                    "mosaic_name": "placeholder",
                    "geometry": "placeholder"
                }
            ]
        }
        self.order_params['products'][0]['geometry'] = geometry
        self.order_params['products'][0]['mosaic_name'] = self.mosaic

        try:
            response = self.session.post(ORDERS_API_URL, 
                                data=json.dumps(self.order_params), 
                                auth=self.auth, 
                                headers={'content-type': 'application/json'})
            order_id = response.json()['id']
            order_url = ORDERS_API_URL + '/' + order_id
            return order_url
        except:
            return response
    
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
    
    def get_links(self, geometry = None):
        try:
            geometry = geometry or self.geometry
            order_url = self.place_order(geometry)
            result = self.poll_for_success(order_url)
            result = result.json()
            return result['_links']['results']
        except:
            if order_url.text[:148] == ERROR_TOO_MANY_QUADS:
                error = json.loads(order_url.text)
                print(error["general"][0]["message"])
                return self.too_many_quads(geometry)
            else:
                print(order_url.text)

    def too_many_quads(self, geometry):
        print("Dividing geometry into smaller parts")
        div_geoms = self.divide_geometry(geometry)
        
        links = []
        for geom in div_geoms:
            r = self.get_links(geom)
            links = links + r[:-1]
        return links


    def divide_geometry(self, geom):
        sumy = 0
        minx = geom['coordinates'][0][0][0]
        maxx = geom['coordinates'][0][0][0]
        for coords in geom['coordinates'][0]:
            if coords[0] < minx:
                minx = coords[0]
            elif coords[0] > maxx:
                maxx = coords[0]
            sumy += coords[1]
        avgy = sumy / len(geom['coordinates'][0])

        poly = shape(geom)
        LINE = f"LINESTRING ({minx-0.1} {avgy}, {maxx+0.1} {avgy})"
        line = wkt.loads(LINE)

        merged = linemerge([poly.boundary, line])
        borders = unary_union(merged)
        polygons = polygonize(borders)

        geoms = []
        for p in polygons:
            geoms.append(to_geojson(p))
        for g in range(len(geoms)):
            geoms[g] = json.loads(geoms[g])

        return geoms

    def download_files(self, num_threads, result, allFiles=False):

        #### num_threads = 0 for max threads possible


        step = 4** (not allFiles)
        result = result[::step]

        if not os.path.exists('downloads'):
            os.mkdir('downloads')

        if(num_threads == 0):
            num_threads = cpu_count()-1
        ThreadPool(num_threads).imap_unordered(download_from_url, result)