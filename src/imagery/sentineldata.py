from .data import Data
from rasterio.io import MemoryFile
from rasterio.crs import CRS
import numpy as np


class SentinelProductList:

    def __init__(self, client, platformname, producttype, processinglevel):
        self._s2_client = client
        self._query_filters = dict()
        self._product_type = (platformname, producttype, processinglevel)
        self.search_result = []

    @property
    def product_type(self):
        if len(self.product_type) == 3:
            return self.product_type
        else:
            return None

    def set_query_filter(self, param, value):
        self.query_filters.update({param: value})

    def _build_search_query(self, offset, page_size):
        if self._query_filters is None and self.product_type is None:
            raise Exception

        self.set_query_filter("platformname", self._product_type[0])
        self.set_query_filter("producttype", self._product_type[1])
        self.set_query_filter("processinglevel", self._product_type[2])

        filters = ' AND '.join(
            [
                f'{key}:{value}'
                for key, value in sorted(self._query_filters.items())
            ]
        )

        return '/search?start={}&rows={}&q={}'.format(offset, page_size, filters)

    def _add_to_products_list(self, xml_bytes):
        root, ns = parse_xml(xml_bytes)
        items_nodes = root.findall('entry', ns)
        for node in items_nodes:
            self.search_result.append(
                f"/Products('{node.find('id', ns).text}')/Nodes('{node.find('title', ns).text}.SAFE')"
            )

    def _get_total_results(self):
        query = self._build_search_query(0, 1)
        response_xml = self._s2_client.api_call(query, stream=False, as_text=True)
        root, ns = parse_xml(response_xml)
        total = int(root.find('opensearch:totalResults', ns).text)
        return total

    def search(self, page_size=10):
        offset = 0
        total = self._get_total_results()
        while True:
            query = self._build_search_query(offset, page_size)
            response_xml = self._s2_client.api_call(query, stream=False, as_text=True)
            self._add_to_products_list(response_xml)
            offset = offset + page_size
            if offset >= total:
                break


class S2MSITiles:
    def __init__(self, product_list, navigator):
        self.products = product_list
        self.navigator = navigator
        self.tiles = []


class MSIBand:
    def __init__(self, name):
        self.name = name
        self.cloud_masks = None
        self.nodata_masks = None
        self.fragments = None
        self.tiles = []
        self.footprint = None


class SentinelData(Data):

    def __init__(self):
        credentials = ('mluzu', 'aufklarung')
        odata_base_url = "https://apihub.copernicus.eu/apihub"
        s2_client = SentinelClient(credentials, odata_base_url)
        self.products_list = SentinelProductList(s2_client)
        self.navigator = S2MSINavigator(credentials, odata_base_url)

        self.products = None
        self.post_filters = {
            'resolution': '10'
        }

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
        self.products_list.set_query_filter('footprint', footprint)

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
        self.products_list.set_query_filter('beginposition', interval)
        self.products_list.set_query_filter('endposition', interval)

    def set_cloudcoverage_filter(self, minpercentage, maxpercentage):
        """
        Prefilter that can be applied by query.
        :param minpercentage:
        :param maxpercentage:
        """
        min_max = f'[{minpercentage} TO {maxpercentage}]'
        self.products_list.set_query_filter('cloudcoverpercentage', min_max)

    def set_bands_filter(self, *bands):
        """
        Postfilter that can be applied by nodes properties
        :params: bands
        """
        if len(bands) == 0:
            raise ValueError("Provide a list of bands")

        self.navigator.set_bands_selector(bands)

    def read(self, count):
        tile = self.fetch_products()
        return tile

    def fetch_products(self, count=0):
        self.products_list.product_type = ('Sentinel-3', 'SL_2_LST___', 'Level-2')
        self.products_list.search(100)
        tile = self.navigator.get_dataset(self.product_list[0])
        return tile
