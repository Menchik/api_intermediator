import requests
from requests.auth import HTTPBasicAuth
import json
import time
from osgeo import ogr

from .base_intermediator import base_intermediator

from multiprocessing.pool import ThreadPool
from multiprocessing import cpu_count
import os

from .utils import download_from_url

from shapely import wkt, to_geojson
from shapely.ops import linemerge, unary_union, polygonize
from shapely.geometry import shape

from sentinelhub import (
    SHConfig,
    CRS,
    BBox,
    DataCollection,
    DownloadRequest,
    MimeType,
    MosaickingOrder,
    SentinelHubDownloadClient,
    SentinelHubRequest,
    bbox_to_dimensions,
)

from .utils import plot_image

class sentinel_intemediator(base_intermediator):
    def __init__(self, auth_key):
        base_intermediator.__init__(self, auth_key)

    def authenticate(self):
        # self.auth = HTTPBasicAuth(self.auth_key, '')
        # self.session = requests.Session()
        # self.session.auth = (self.auth_key, '')
        self.config = SHConfig()
        self.config.sh_token_url = "https://identity.dataspace.copernicus.eu/auth/realms/CDSE/protocol/openid-connect/token"
        self.config.sh_base_url = "https://sh.dataspace.copernicus.eu"
        self.config.sh_client_id = 'sh-4d4f09a8-8175-464e-ab24-5abbe5fac426'
        self.config.sh_client_secret = self.auth_key

    def set_AOI_from_geometry(self, geometry):
        self.geometry = geometry

    def set_AOI_from_geojson(self, file_path):
        file = open(file_path)
        geojson = json.load(file)
        self.set_AOI_from_geometry(geojson['features'][0]['geometry'])

    def set_AOI_from_shapefile(self, file_path):
        ogr.DontUseExceptions()

        geom = []

        shapefile = ogr.Open(file_path)
        layer = shapefile.GetLayer()

        for feature in layer:
            geometry = feature.GetGeometryRef() 
            geometry = json.loads(geometry.ExportToJson())
            
            geom.append(geometry)

        print(geom[0])
        print(geom[0]['coordinates'])
        print(f"{geom[0]['coordinates'][0][0]}, {geom[0]['coordinates'][0][1]}")
        self.set_AOI_from_geometry(geom[0])

    def set_resolutionBox(self, value=10): 
        box = self.geometry['coordinates'][0][0] + self.geometry['coordinates'][0][1]
        
        resolution = value
        self.aoi_bbox = BBox(bbox = box, crs=CRS.WGS84)
        self.aoi_size = bbox_to_dimensions(self.aoi_bbox, resolution=resolution)

        print(f"Image shape at {resolution} m resolution: {self.aoi_size} pixels")

    def download(self, time_interval):
        evalscript_true_color = """
            //VERSION=3

            function setup() {
                return {
                    input: [{
                        bands: ["B02", "B03", "B04"]
                    }],
                    output: {
                        bands: 3
                    }
                };
            }

            function evaluatePixel(sample) {
                return [sample.B04, sample.B03, sample.B02];
            }
        """

        request_true_color = SentinelHubRequest(
            evalscript=evalscript_true_color,
            input_data=[
                SentinelHubRequest.input_data(
                    data_collection=DataCollection.SENTINEL2_L1C.define_from(
                        "s2l1c", service_url= self.config.sh_base_url
                    ),
                    time_interval=time_interval,
                )
            ],
            responses=[SentinelHubRequest.output_response("default", MimeType.PNG)],
            bbox = self.aoi_bbox,
            size = self.aoi_size,
            config = self.config,
        )

        true_color_imgs = request_true_color.get_data()

        print(f"Returned data is of type = {type(true_color_imgs)} and length {len(true_color_imgs)}.")
        print(f"Single element in the list is of type {type(true_color_imgs[-1])} and has shape {true_color_imgs[-1].shape}")

        image = true_color_imgs[0]
        print(f"Image type: {image.dtype}")

        # plot function
        # factor 1/255 to scale between 0-1
        # factor 3.5 to increase brightness
        plot_image(image, factor=3.5 / 255, clip_range=(0, 1))


    def download_files(self, num_threads, result, allFiles=False):

        #### num_threads = 0 for max threads possible


        step = 4** (not allFiles)
        result = result[::step]

        if not os.path.exists('downloads'):
            os.mkdir('downloads')

        if(num_threads == 0):
            num_threads = cpu_count()-1
        ThreadPool(num_threads).imap_unordered(download_from_url, result)