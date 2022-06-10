from ..data import Data
from .client import SentinelClient
from .search import SentinelProductList
from typing import Final
from enum import Enum
import fnmatch
from rasterio import MemoryFile
from rasterio.plot import reshape_as_image
from rasterio.crs import CRS
from s2cloudless import S2PixelCloudDetector
import numpy as np


class ProductType(Enum):
    S2MSI: Final[tuple] = ('Sentinel-2', ['S2MSI2A', 'S2MSI1C'], ['Level-2A', 'Level-1C'])


class SentinelData(Data):

    def __init__(self, product_type, feature):
        credentials = ('mluzu', 'aufklarung')
        client = SentinelClient(credentials)
        self.products_list = SentinelProductList(client, product_type)
        self.navigator = S2MSINavigator(client)
        self.feature = feature

    def set_bounds_filter(self, rect):
        if len(rect) != 4:
            raise ValueError("Only supports AOI of polygons")

        roi = '({} {}, {} {}, {} {}, {} {}, {} {})'.format(rect[0], rect[1], rect[2], rect[1], rect[2],
                                                           rect[3], rect[0], rect[3], rect[0], rect[1])
        footprint = '"Intersects(POLYGON({}))"'.format(roi)
        self.products_list.add_query_filter('footprint', footprint)

    def set_date_filter(self, begintime, endtime):
        if begintime is None and endtime is None:
            raise ValueError("Provide a begin date and end date")

        interval = f'[{begintime} TO {endtime}]'
        self.products_list.add_query_filter('beginposition', interval)
        self.products_list.add_query_filter('endposition', interval)

    def set_cloudcoverage_filter(self, minpercentage, maxpercentage):
        min_max = f'[{minpercentage} TO {maxpercentage}]'
        self.products_list.add_query_filter('cloudcoverpercentage', min_max)

    def set_bands_filter(self, *bands):
        if len(bands) == 0:
            raise ValueError("Provide a list of bands")

        self.navigator.bands = bands

    def get(self):
        for tile_descriptor in self.products_list:
            self.navigator.load(tile_descriptor)
            # creo la mascara de nubes con level 1c y s2cloudless
            cloud_prob, cloud_mask = create_cloud_mask(self.navigator)
            # otengo las bandas del level 2A
            #bands = self.navigator.get_level2a_bands(bands)
            # limpio los pixeles con nube
            #bands = clean_clouds(bands, cloud_mask)
            # recorto las bandas
            #bands = crop_bands(bands, self.feature)
            # mergeo en el dataset
            return cloud_prob, cloud_mask


class S2MSINavigator:

    def __init__(self, client):
        self.client = client
        self.current_tile = None
        self.bands = []
        self.img_files = []
        self.qi_files = []
        self.img_1c_files = []
        self.qi_1c_files = []
        self.metadata_2a_file = str
        self.metadata_1c_file = str

    def load(self, tile_descriptor):
        # load Level-2A file paths in manifest
        manifest_path = tile_descriptor.manifest_path_level2a()
        files = self._files_from_manifest(manifest_path)
        self.img_files = fnmatch.filter(files, "./GRANULE/*/IMG_DATA/*")
        self.qi_files = fnmatch.filter(files, "./GRANULE/*/QI_DATA/*")
        self.metadata_2a_file = fnmatch.filter(files, "./GRANULE/*/MTD_TL.xml")[0]

        # load Level-2A file paths in manifest
        manifest_path = tile_descriptor.manifest_path_level1c()
        files = self._files_from_manifest(manifest_path)
        self.img_1c_files = fnmatch.filter(files, "./GRANULE/*/IMG_DATA/*")
        self.qi_1c_files = fnmatch.filter(files, "./GRANULE/*/QI_DATA/*")
        self.metadata_1c_file = fnmatch.filter(files, "./GRANULE/*/MTD_TL.xml")[0]

        self.current_tile = tile_descriptor

    def get_level1c_metadata(self):
        metadata_path = self.build_1c_metadata_path()
        reader = self.client.get(metadata_path)
        tile_geo = reader.get_node("n1:Geometric_Info/Tile_Geocoding")
        tile_crs = tile_geo.get_value('HORIZONTAL_CS_CODE')
        sizes = tile_geo.get_all('Size')
        return {
            'crs': CRS.from_string(tile_crs),
            '10': (sizes[0].get_value('NROWS', int), sizes[0].get_value('NCOLS', int)),
            '20': (sizes[1].get_value('NROWS', int), sizes[1].get_value('NCOLS', int)),
            '60': (sizes[2].get_value('NROWS', int), sizes[2].get_value('NCOLS', int)),
        }

    def _files_from_manifest(self, manifest_path):
        reader = self.client.get(manifest_path)
        files_locations = []
        for location in reader.get_all("dataObjectSection/dataObject/byteStream/fileLocation", use_namespace=False):
            loc = location.get_node_attr('.', 'href')
            files_locations.append(loc)
        return files_locations

    def build_1c_metadata_path(self):
        rel_dirs = self.metadata_1c_file.lstrip('./').split('/')
        navigator_path = '/'.join(
            [
                f"Nodes('{rel}')"
                for rel in rel_dirs
            ]
        )
        return "/Products('{}')/Nodes('{}')/{}/$value".format(self.current_tile.level1C_uuid,
                                                              self.current_tile.level1C_filename,
                                                              navigator_path)

    def build_1c_granule_path(self, band):
        file_path = fnmatch.filter(self.img_1c_files, f'./GRANULE/*/IMG_DATA/*_{band}.jp2')
        rel_dirs = file_path[0].lstrip('./').split('/')
        navigator_path = '/'.join(
            [
                f"Nodes('{rel}')"
                for rel in rel_dirs
            ]
        )
        return "/Products('{}')/Nodes('{}')/{}/$value".format(self.current_tile.level1C_uuid,
                                                              self.current_tile.level1C_filename,
                                                              navigator_path)

    def fetch_level1c_band(self, band, meta):
        file_path = self.build_1c_granule_path(band)
        image_bytes = self.client.stream(file_path)
        profile = {
            'driver': 'JP2OpenJPEG',
            'dtype': 'uint16',
            'nodata': None,
            'width': meta['10'][0],
            'height': meta['10'][1],
            'count': 1,
            'crs': meta['crs'],
        }
        mem_file = MemoryFile(image_bytes)
        img = mem_file.open(**profile).read()
        return img

    def get_level1c_bands(self, *bands):
        image_bands = []
        meta = self.get_level1c_metadata()
        for band in bands:
            img = self.fetch_level1c_band(band, meta)
            image_bands.append(img)
        out = np.ndarray(image_bands)
        return reshape_as_image(out)

    def get_level2a_bands(self, bands):
        return bands


def create_cloud_mask(navigator):
    bands = navigator.get_level1c_bands('B01', 'B02', 'B04', 'B05', 'B08', 'B8A', 'B09', 'B10', 'B11', 'B12')
    cloud_detector = S2PixelCloudDetector(threshold=0.4, average_over=4, dilation_size=2, all_bands=False)
    cloud_prob = cloud_detector.get_cloud_probability_maps(bands)
    cloud_mask = cloud_detector.get_cloud_masks(bands)
    return cloud_prob, cloud_mask,


def crop_bands(bands, feature):
    return 0


def clean_clouds(navigator, cloud_mask):
    return 0
