import requests
import json
import os
import sys
import csv
import pyproj
import geopandas as gpd
from shapely.geometry import shape
from shapely.geometry import box
from shapely.geometry import Polygon
from shapely.ops import transform
from pySmartDL import SmartDL
from datetimerange import DateTimeRange
from functools import partial

from .base_intermediator import base_intermediator

class planet_intermediator(base_intermediator):
    def __init__(self, auth_key=None):
        self.PL_API_KEY = auth_key
        base_intermediator.__init__(self)

    def authenticate(self):
        try:
            if not self.PL_API_KEY:
                self.PL_API_KEY = os.getenv('PL_API_KEY')
                
            self.SESSION = requests.Session()
            self.SESSION.auth = (self.PL_API_KEY, '')
        except Exception as e:
            print(e)
            print('Failed to get Planet Key: Initialize First')

    def shp2geojson(self, folder, export):
        for items in os.listdir(folder):
            if items.endswith('.shp'):
                inD = gpd.read_file(os.path.join(folder,items))
                #Reproject to EPSG 4326
                try:
                    data_proj = inD.copy()
                    data_proj['geometry'] = data_proj['geometry'].to_crs(epsg=4326)
                    data_proj.to_file(os.path.join(export,str(items).replace('.shp', '.geojson')), driver="GeoJSON")
                    print('Export completed to '+str(os.path.join(export,str(items).replace('.shp', '.geojson'))))
                except Exception as e:
                    print(e)

    def print_bbox(self, infile):
        #Create an empty geojson template
        temp={"coordinates":[],"type":"Polygon"}
        ##Parse Geometry
        try:
            if infile.endswith('.geojson'):
                with open(infile) as aoi:
                    aoi_resp = json.load(aoi)
                    aoi_geom = aoi_resp['features'][0]['geometry']['coordinates']
            elif infile.endswith('.json'):
                with open (infile) as aoi:
                    aoi_resp=json.load(aoi)
                    aoi_geom=aoi_resp['config'][0]['config']['coordinates']
        except Exception as e:
            print('Could not parse geometry')
            print(e)
        except (KeyboardInterrupt, SystemExit) as e:
            print('Program escaped by User')
            sys.exit()
        temp['coordinates'] = aoi_geom
        gmain = shape(temp)
        gmainbound = (','.join(str(v) for v in list(gmain.bounds)))
        print('')
        print('rbox:')
        print(str(gmainbound))

    def handle_page_mosaics(self, response, gmainbound,start, end,outfile):
        for items in response['mosaics']:
            bd = items['bbox']
            mosgeom = shape(Polygon(box(bd[0], bd[1], bd[2], bd[3]).exterior.coords))
            gboundlist = gmainbound.split(',')
            boundgeom = shape(Polygon(box(float(gboundlist[0]), float(gboundlist[1]), float(gboundlist[2]), float(gboundlist[3]))))
            proj = partial(pyproj.transform, pyproj.Proj(init='epsg:4326'), pyproj.Proj(init='epsg:3857'))
            boundgeom = transform(proj, boundgeom)
            mosgeom = transform(proj, mosgeom)
            if boundgeom.intersection(mosgeom).is_empty:
                pass
            else:
                id = items['id']
                r = requests.get('https://api.planet.com/mosaic/experimental/mosaics/' + str(id) + '/quads?bbox=' + str(gboundlist[0])+'%2C'+gboundlist[1]+'%2C'+gboundlist[2]+'%2C'+gboundlist[3],auth=(self.PL_API_KEY,''))
                resp = r.json()
                if len(resp['items']) > 0:
                    time_range = DateTimeRange(items['first_acquired'].split('T')[0], items['last_acquired'].split('T')[0])
                    x = DateTimeRange(start, end)
                    if time_range.is_intersection(x) is True:
                        #print(boundgeom.intersection(mosgeom).area/1000000)
                        print('Mosaic name:  ' + str(items['name']))
                        print('Mosaic Resolution:  ' + str(items['grid']['resolution']))
                        print('Mosaic ID:  ' + str(items['id']))
                        name=str(items['name'])
                        ids=str(items['id'])
                        facq=str(items['first_acquired']).split('T')[0]
                        lacq=str(items['last_acquired']).split('T')[0]
                        res=str(items['grid']['resolution'])
                        print('')
                        with open(outfile,'a') as csvfile:
                            writer=csv.writer(csvfile,delimiter=',',lineterminator='\n')
                            writer.writerow([name, ids, facq,lacq,format(float(res),'.3f')])
                        csvfile.close()

    def get_mosaics(self, infile, start, end, outfile):
        #Create an empty geojson template
        temp={"coordinates":[],"type":"Polygon"}
        headers = {'Content-Type': 'application/json'}

        with open(outfile,'w') as csvfile:
            writer=csv.DictWriter(csvfile,fieldnames=["name", "id", "first_acquired",
                                                    "last_acquired","resolution"], delimiter=',')
            writer.writeheader()
        ##Parse Geometry
            try:
                if infile.endswith('.geojson'):
                    with open(infile) as aoi:
                        aoi_resp = json.load(aoi)
                        aoi_geom = aoi_resp['features'][0]['geometry']['coordinates']
                elif infile.endswith('.json'):
                    with open (infile) as aoi:
                        aoi_resp=json.load(aoi)
                        aoi_geom=aoi_resp['config'][0]['config']['coordinates']
                # elif infile.endswith('.kml'):
                #     getcoord=kml2coord(infile)
                #     aoi_geom=getcoord
            except Exception as e:
                print('Could not parse geometry')
                print(e)

            temp['coordinates'] = aoi_geom
            gmain = shape(temp)
            gmainbound = (','.join(str(v) for v in list(gmain.bounds)))
            print('rbox:' + str(gmainbound)+'\n')
            r = requests.get('https://api.planet.com/basemaps/v1/mosaics', auth=(self.PL_API_KEY, ''))
            response = r.json()
            print(response)
            try:
                if response['mosaics'][0]['quad_download'] ==True:
                    final_list = self.handle_page_mosaics(response, gmainbound, start, end,outfile)
            except KeyError:
                print('No Download permission for: '+str(response['mosaics'][0]['name']))
            try:
                while response['_links'].get('_next') is not None:
                    page_url = response['_links'].get('_next')
                    r = requests.get(page_url)
                    response = r.json()
                    print(response)
                    try:
                        if response['mosaics'][0]['quad_download'] ==True:
                            final_list = self.handle_page_mosaics(response, gmainbound, start, end,outfile)
                    except KeyError:
                        print('No Download permission for: '+str(response['mosaics'][0]['name']))
            except Exception as e:
                print(e)
            except (KeyboardInterrupt, SystemExit) as e:
                print('Program escaped by User')
                sys.exit()


    # Function to download the geotiffs
    def download(self, ids, names, idlist, infile, coverage, local):
        if idlist is None and names is not None:
            self.downloader_download(ids,names, infile, coverage, local)
        elif idlist is not None:
            with open(idlist) as csvfile:
                reader=csv.DictReader(csvfile)
                for row in reader:
                    print('')
                    print('Processing: '+str(row['name']))
                    self.downloader_download(str(row['id']),str(row['name']),infile, coverage, local)

    #Check running orders
    def hpage_download(self, page,names,coverage, local):
        try:
            for things in page['items']:
                downlink=(things['_links']['download'])
                if coverage is not None and int(things['percent_covered']) >= int(coverage):
                    r = requests.get(downlink,allow_redirects=False)
                    filelink = r.headers['Location']
                    filename = str(r.headers['Location']).split('%22')[-2]
                    fpath=os.path.join(local,names)
                    if not os.path.exists(fpath):
                        os.makedirs(fpath)
                    localpath = os.path.join(fpath,filename)
                    result = self.SESSION.get(filelink)
                    if not os.path.exists(localpath) and result.status_code == 200:
                        print("Downloading: " + str(localpath))
                        f = open(localpath, 'wb')
                        for chunk in result.iter_content(chunk_size=512 * 1024):
                            if chunk:
                                f.write(chunk)
                        f.close()
                    else:
                        if int(result.status_code) != 200:
                            print("Encountered error with code: " + str(result.status_code) + ' for ' + str(localpath))
                        elif int(result.status_code) == 200:
                            print("File already exists SKIPPING: " + str(localpath))
                elif coverage is None:
                    downlink = things['_links']['download']
                    r = requests.get(downlink,allow_redirects=False)
                    filelink=r.headers['Location']
                    filename=str(r.headers['Location']).split('%22')[-2]
                    fpath=os.path.join(local,names)
                    if not os.path.exists(fpath):
                        os.makedirs(fpath)
                    localpath = os.path.join(fpath,filename)
                    result = self.SESSION.get(filelink)
                    if not os.path.exists(localpath) and result.status_code == 200:
                        print("Downloading: " + str(localpath))
                        f = open(localpath, 'wb')
                        for chunk in result.iter_content(chunk_size=512 * 1024):
                            if chunk:
                                f.write(chunk)
                        f.close()
                    else:
                        if int(result.status_code) != 200:
                            print("Encountered error with code: " + str(result.status_code) + ' for ' + str(localpath))
                        elif int(result.status_code) == 200:
                            print("File already exists SKIPPING: " + str(localpath))
        except Exception as e:
            print(e)
        except (KeyboardInterrupt, SystemExit) as e:
            print('Program escaped by User')
            sys.exit()


    # Get item id from item name
    def handle_page_download(names,response):
        for items in response['mosaics']:
            if items['name']==names:
                return items['id']

    # Downloader
    def downloader_download(self, ids, names, infile, coverage, local):
        idmatch=[]
        # Create an empty geojson template
        temp = {"coordinates":[], "type":"Polygon"}
        CAS_URL = 'https://api.planet.com/mosaic/experimental/mosaics/'
        
        if names is None and ids is not None:
            ids=ids
        elif names is not None and ids is None:
            resp=self.SESSION.get('https://api.planet.com/basemaps/v1/mosaics')
            response=resp.json()
            ids=self.handle_page_download(names,response)
            idmatch.append(ids)
            try:
                while response['_links'].get('_next') is not None:
                    page_url = response['_links'].get('_next')
                    r = requests.get(page_url)
                    response = r.json()
                    ids = self.handle_page_download(names,response)
                    idmatch.append(ids)
            except Exception as e:
                print(e)
            for ival in idmatch:
                if ival is not None:
                    ids=ival
        elif names is not None and ids is not None:
            ids = ids
        headers = {'Content-Type': 'application/json'}
        try:
            if infile.endswith('.geojson'):
                with open(infile) as aoi:
                    aoi_resp = json.load(aoi)
                    aoi_geom = aoi_resp['features'][0]['geometry']['coordinates']
            elif infile.endswith('.json'):
                with open (infile) as aoi:
                    aoi_resp=json.load(aoi)
                    aoi_geom=aoi_resp['config'][0]['config']['coordinates']
            # elif infile.endswith('.kml'):
            #     getcoord=kml2coord(infile)
            #     aoi_geom=getcoord
        except Exception as e:
            print('Could not parse geometry')
            print(e)

        temp['coordinates'] = aoi_geom
        gmain = shape(temp)
        gmainbound = (','.join(str(v) for v in list(gmain.bounds)))
        gboundlist = gmainbound.split(',')
        url = CAS_URL \
            + str(ids) + '/quads?bbox=' + str(gboundlist[0]) \
            + '%2C' + str(gboundlist[1]) + '%2C' + str(gboundlist[2]) \
            + '%2C' + str(gboundlist[3])
        #print(url)
        main = self.SESSION.get(url)
        if main.status_code == 200:
            page=main.json()
            self.hpage_download(page,names,coverage, local)
            while page['_links'].get('_next') is not None:
                try:
                    page_url = page['_links'].get('_next')
                    result = self.SESSION.get(page_url)
                    if result.status_code == 200:
                        page=result.json()
                        self.hpage_download(page,names,coverage, local)
                    else:
                        print(result.status_code)
                except Exception as e:
                    pass
                except (KeyboardInterrupt, SystemExit) as e:
                    print('Program escaped by User')
                    sys.exit()

    # Function to download the geotiffs
    def multipart(self, ids,names, idlist, infile, coverage, local):
        if idlist is None and names is not None:
            self.downloader_multipart(ids,names, infile, coverage, local)
        elif idlist is not None:
            with open(idlist) as csvfile:
                reader=csv.DictReader(csvfile)
                for row in reader:
                    print('')
                    print('Processing: '+str(row['name']))
                    self.downloader_multipart(str(row['id']),str(row['name']),infile, coverage, local)

    #Check running orders
    def hpage_multipart(self, page,names,coverage, local):
        try:
            for things in page['items']:
                downlink=(things['_links']['download'])
                if coverage is not None and int(things['percent_covered']) >= int(coverage):
                    r = requests.get(downlink,allow_redirects=False)
                    filelink = r.headers['Location']
                    filename = str(r.headers['Location']).split('%22')[-2]
                    fpath=os.path.join(local,names)
                    if not os.path.exists(fpath):
                        os.makedirs(fpath)
                    localpath = os.path.join(fpath,filename)
                    if not os.path.exists(localpath):
                        print("Downloading: " + str(localpath))
                        obj = SmartDL(filelink, localpath)
                        obj.start()
                        path = obj.get_dest()
                    else:
                        print("File already exists SKIPPING: " + str(localpath))
                elif coverage is None:
                    downlink = things['_links']['download']
                    r = requests.get(downlink,allow_redirects=False)
                    filelink=r.headers['Location']
                    filename=str(r.headers['Location']).split('%22')[-2]
                    fpath=os.path.join(local,names)
                    if not os.path.exists(fpath):
                        os.makedirs(fpath)
                    localpath = os.path.join(fpath,filename)
                    if not os.path.exists(localpath):
                        print("Downloading: " + str(localpath))
                        obj = SmartDL(filelink, localpath)
                        obj.start()
                        path = obj.get_dest()
                    else:
                        print("File already exists SKIPPING: " + str(localpath))
        except Exception as e:
            print(e)
        except (KeyboardInterrupt, SystemExit) as e:
            print('Program escaped by User')
            sys.exit()


    # Get item id from item name
    def handle_page_multipart(self, names,response):
        for items in response['mosaics']:
            if items['name']==names:
                return items['id']

    # Downloader
    def downloader_multipart(self, ids,names, infile, coverage, local):
        idmatch=[]
        # Create an empty geojson template
        temp = {"coordinates":[], "type":"Polygon"}
        CAS_URL = 'https://api.planet.com/mosaic/experimental/mosaics/'

        if names is None and ids is not None:
            ids=ids
        elif names is not None and ids is None:
            resp=self.SESSION.get('https://api.planet.com/basemaps/v1/mosaics')
            response=resp.json()
            ids=self.handle_page_multipart(names,response)
            idmatch.append(ids)
            try:
                while response['_links'].get('_next') is not None:
                    page_url = response['_links'].get('_next')
                    r = requests.get(page_url)
                    response = r.json()
                    ids = self.handle_page_multipart(names,response)
                    idmatch.append(ids)
            except Exception as e:
                print(e)
            for ival in idmatch:
                if ival is not None:
                    ids=ival
        elif names is not None and ids is not None:
            ids = ids
        headers = {'Content-Type': 'application/json'}
        try:
            if infile.endswith('.geojson'):
                with open(infile) as aoi:
                    aoi_resp = json.load(aoi)
                    aoi_geom = aoi_resp['features'][0]['geometry']['coordinates']
            elif infile.endswith('.json'):
                with open (infile) as aoi:
                    aoi_resp=json.load(aoi)
                    aoi_geom=aoi_resp['config'][0]['config']['coordinates']
            # elif infile.endswith('.kml'):
            #     getcoord=kml2coord(infile)
            #     aoi_geom=getcoord
        except Exception as e:
            print('Could not parse geometry')
            print(e)

        temp['coordinates'] = aoi_geom
        gmain = shape(temp)
        gmainbound = (','.join(str(v) for v in list(gmain.bounds)))
        gboundlist = gmainbound.split(',')
        url = CAS_URL \
            + str(ids) + '/quads?bbox=' + str(gboundlist[0]) \
            + '%2C' + str(gboundlist[1]) + '%2C' + str(gboundlist[2]) \
            + '%2C' + str(gboundlist[3])
        #print(url)
        main = self.SESSION.get(url)
        if main.status_code == 200:
            page=main.json()
            self.hpage_multipart(page,names,coverage, local)
            while page['_links'].get('_next') is not None:
                try:
                    page_url = page['_links'].get('_next')
                    result = self.SESSION.get(page_url)
                    if result.status_code == 200:
                        page=result.json()
                        self.hpage_multipart(page,names,coverage, local)
                    else:
                        print(result.status_code)
                except Exception as e:
                    pass
                except (KeyboardInterrupt, SystemExit) as e:
                    print('Program escaped by User')
                    sys.exit()
