from ..data import Data
from .client import SentinelClient
from .search import SentinelProductList
from typing import Final
from enum import Enum
import fnmatch


class ProductType(Enum):
    S2MSI: Final[tuple] = ('Sentinel-2', ['S2MSI2A', 'S2MSI1C'], ['Level-2A',  'Level-1C'])


class SentinelData(Data):

    def __init__(self, product_type):
        credentials = ('mluzu', 'aufklarung')
        client = SentinelClient(credentials)
        self.products_list = SentinelProductList(client, product_type)
        self.navigator = S2MSINavigator(client)
        self._gross_filter = Filter()

    def set_bounds_filter(self, rect):
        """
        Prefilter that can be applied by query. The region of interest is calculated from a rectangle diagonal
        :param rect: Only sequence type is with four elements supported
        """
        if len(rect) != 4:
            raise ValueError("Only supports AOI of polygons")

        roi = '({} {}, {} {}, {} {}, {} {}, {} {})'.format(rect[0], rect[1], rect[2], rect[1], rect[2],
                                                           rect[3], rect[0], rect[3], rect[0], rect[1])
        footprint = '"Intersects(POLYGON({}))"'.format(roi)
        self.products_list.add_query_filter('footprint', footprint)

    def set_date_filter(self, begintime, endtime):
        """
        Prefilter that can be applied by query.
        Supported formats are:
        - yyyyMMdd
        - yyyy-MM-ddThh:mm:ss.SSSZ (ISO-8601)
        - yyyy-MM-ddThh:mm:ssZ
        - NOW
        - NOW-<n>DAY(S) (or HOUR(S), MONTH(S), etc.)
        - NOW+<n>DAY(S)
        - yyyy-MM-ddThh:mm:ssZ-<n>DAY(S)
        - NOW/DAY (or HOUR, MONTH etc.) - rounds the value to the given unit
        :param begintime:
        :param endtime:
        """
        if begintime is None and endtime is None:
            raise ValueError("Provide a begin date and end date")

        interval = f'[{begintime} TO {endtime}]'
        self.products_list.add_query_filter('beginposition', interval)
        self.products_list.add_query_filter('endposition', interval)

    def set_cloudcoverage_filter(self, minpercentage, maxpercentage):
        """
        Prefilter that can be applied by query.
        :param minpercentage:
        :param maxpercentage:
        """
        min_max = f'[{minpercentage} TO {maxpercentage}]'
        self.products_list.add_query_filter('cloudcoverpercentage', min_max)

    def set_bands_filter(self, *bands):
        """
        Postfilter that can be applied by nodes properties
        :params: bands
        """
        if len(bands) == 0:
            raise ValueError("Provide a list of bands")

        self.navigator.bands = bands

    def get(self):
        t = next(self.products_list)
        self.navigator.load(t)
        return self.navigator.current_tile

    def fetch_products(self, count=0):
        pass

    def pass_gross_filter(self):
        new_list = []
        for product in self.products_list:
            self.navigator.load(product)
            self._gross_filter.evaluate(self.navigator)

        if self._gross_filter.passed():
            new_list.append(product)


class S2MSINavigator:

    def __init__(self, client, shapes):
        self.client = client
        self.shapes = shapes
        self.current_tile = None
        self.bands = []
        self.img_files = []
        self.qi_files = []
        self.img_1c_files = []
        self.qi_1c_files = []

    def load(self, tile_descriptor):
        # load Level-2A file paths in manifest
        manifest_path = tile_descriptor.manifest_path_level2a()
        files = self._files_from_manifest(manifest_path)
        self.img_files = fnmatch.filter(files, "./GRANULE/*/IMG_DATA/*")
        self.qi_files = fnmatch.filter(files, "./GRANULE/*/QI_DATA/*")

        # load Level-2A file paths in manifest
        manifest_path = tile_descriptor.manifest_path_level1c()
        files = self._files_from_manifest(manifest_path)
        self.img_1c_files = fnmatch.filter(files, "./GRANULE/*/IMG_DATA/*")
        self.qi_1c_files = fnmatch.filter(files, "./GRANULE/*/QI_DATA/*")

        self.current_tile = tile_descriptor

    def _files_from_manifest(self, manifest_path):
        reader = self.client.get(manifest_path)
        files_locations = []
        for location in reader.get_all("dataObjectSection/dataObject/byteStream/fileLocation", use_namespace=False):
            loc = location.get_node_attr('.', 'href')
            files_locations.append(loc)
        return files_locations


class Filter:
    def __init__(self, shapes):
        self.shapes = shapes

    def evaluate(self, navigator):
        pass

    def passed(self):
        pass


class CloudFilter:
    def __init__(self, shapes):
        self.shapes = shapes

    def sadf(self, navigator):
        cloud_mask = navigator.get_l1c_cloud_mask()
