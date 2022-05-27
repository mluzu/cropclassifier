import xml.etree.ElementTree as xml
from .data import Data
from rasterio.io import MemoryFile
from rasterio.crs import CRS
from requests import Session
from requests.exceptions import HTTPError
from .exceptions import SentinelAPIError
import numpy as np
import re
import fnmatch


def namespace(element):
    m = re.match(r'\{.*\}', element.tag)
    return m.group(0) if m else ''


def parse_xml(xml_bytes):
    root = xml.fromstring(xml_bytes)
    ns = namespace(root)
    return root, ns


class SentinelAPI:

    def __init__(self, credentials, odata_base_url):
        self.credentials = credentials
        self.session = Session()
        self.odata_base_url = odata_base_url

    def api_call(self, query, stream):
        url = '{}{}'.format(self.odata_base_url, query)
        try:
            with self.session.get(url, auth=self.credentials, stream=stream) as response:
                if response.status_code == 200:
                    return response.content
                response.raise_for_status()
        except HTTPError:            raise SentinelAPIError("Failed request to SentinelApi", response)


class SentinelOpenSearch(SentinelAPI):

    def __init__(self, credentials, odata_base_url):
        super().__init__(credentials, odata_base_url)
        self.query_filters = dict()
        self.search_result = None

    def _build_search_query(self):
        if self.query_filters is None:
            raise Exception

        filters = ' AND '.join(
            [
                f'{key}:{value}'
                for key, value in sorted(self.query_filters.items())
            ]
        )

        return '/search?q={}'.format(filters)

    def set_filter(self, param, value):
        self.query_filters.update({param: value})

    @staticmethod
    def build_products_list(response_bytes):
        root, ns = parse_xml(response_bytes)
        products_nodes = root.iter(f'{ns}entry')

        products_list = []
        for node in products_nodes:
            products_list.append(
                f"/Products('{node.find(f'{ns}id').text}')/Nodes('{node.find(f'{ns}title').text}.SAFE')"
            )

        return products_list

    # TODO: pagination support is required. Now using default page size
    # TODO: filter at product level can reduce posterior computational effort
    def search(self):
        query = self._build_search_query()
        response_bytes = self.api_call(query, False)
        return self.build_products_list(response_bytes)


class S2MSINavigator(SentinelAPI):

    def __init__(self, credentials, odata_base_url):
        super().__init__(credentials, odata_base_url)
        self.odata_path = "/odata/v1"
        self.products_list = list()
        self.selector = dict()
        self.manifest = None
        self.product_metadata = None
        self.granule_metadata = None
        self.granule_list = []
        self.set_resolution_selector(20)

    def set_bands_selector(self, bands):
        self.selector.update({"bands": bands})

    def set_resolution_selector(self, resolution):
        self.selector.update({"resolution": resolution})

    def _get_manifest(self, product_node):
        query = "{}{}/Nodes('manifest.safe')/$value".format(self.odata_path, product_node)
        response_bytes = self.api_call(query, False)
        self.manifest = xml.fromstring(response_bytes)

    def _get_product_metadata(self, product_node):
        query = "{}{}/Nodes('MTD_MSIL2A.xml')/$value".format(self.odata_path, product_node)
        response_bytes = self.api_call(query, False)
        self.product_metadata = xml.fromstring(response_bytes)

    def _get_granule_metadata(self, product_node, granule_title):
        url = "{}{}/Nodes('GRANULE')/Nodes('{}')/Nodes('MTD_TL.xml')/$value".format(
            self.odata_path, product_node, granule_title
        )
        response_bytes = self.api_call(url, False)
        self.granule_metadata = xml.fromstring(response_bytes)

    def _get_granule_title(self, product_node):
        url = "{}{}/Nodes('GRANULE')/Nodes".format(self.odata_path, product_node)
        response_bytes = self.api_call(url, False)
        root, ns = parse_xml(response_bytes)
        return root.find(f'{ns}entry').find(f'{ns}title').text

    def _get_manifest_granule_list(self):
        if self.manifest is None:
            raise Exception
        regex = re.compile('IMG_DATA*')
        granule_list = []
        data_object_list = self.manifest.find('dataObjectSection').iter('dataObject')
        for item in data_object_list:
            if re.match(regex, item.attrib.get('ID')):
                file_location = item.find('.//fileLocation')
                href = file_location.attrib.get('href')
                href = href.lstrip('.')
            granule_list.append(href)
        return granule_list

    def _get_metadata_granule_list(self):
        if self.product_metadata is None:
            raise Exception

        return [item.text for item in self.product_metadata.findall('.//IMAGE_FILE')]

    def _get_metadata_profile(self, resolution):
        if self.granule_metadata is None:
            raise Exception

        ns = namespace(self.granule_metadata)
        profile = dict()
        tile_info = self.granule_metadata.find(f'./{ns}Geometric_Info/Tile_Geocoding')
        cs_name = tile_info.find('HORIZONTAL_CS_NAME')
        profile.update({"crs_name": cs_name.text})
        cs_code = tile_info.find('HORIZONTAL_CS_CODE')
        profile.update({"crs_name": cs_code.text})
        sizes = tile_info.iter('Size')
        for size in sizes:
            r = size.attrib.get('resolution')
            if int(r) == resolution:
                profile.update({
                    f'size': (int(size.find('NROWS').text), int(size.find('NCOLS').text))
                })

        return profile

    def filter_granules_by_resolution(self, granule_list, bands, resolution):
        files = []
        pattern = f'*_{resolution}m'
        files_by_resolution = fnmatch.filter(granule_list, pattern)
        for band in bands:
            pattern = f'*_{band}_*'
            for file in files_by_resolution:
                if fnmatch.fnmatch(file, pattern):
                    files.append(file)
        return files

    def _get_bands(self, product_node, granule_list, resolution):
        profile = self._get_metadata_profile(resolution)
        size = profile.get('size')

        profile.update({
            'driver': 'JP2OpenJPEG',
            'dtype': 'uint16',
            'nodata': None,
            'width': size[0],
            'height': size[1],
            'count': 1,
            'crs': CRS.from_epsg(32617),
        })

        for i, file_path in enumerate(granule_list):
            granule, identifier, img_folder, res_folder, file_name = file_path.split('/')
            query = "{}{}/Nodes('{}')/Nodes('{}')/Nodes('{}')/Nodes('{}')/Nodes('{}.jp2')/$value"\
                .format(self.odata_path, product_node, granule, identifier, img_folder, res_folder, file_name)
            image_bytes = self.api_call(query, stream=True)
            with MemoryFile(image_bytes) as mem_file:
                with mem_file.open(**profile) as dataset:
                    return dataset

    def get_dataset(self, product_node):
        self._get_manifest(product_node)
        self._get_product_metadata(product_node)
        granule_title = self._get_granule_title(product_node)
        self._get_granule_metadata(product_node, granule_title)

        resolution = self.selector.get('resolution')
        profile = self._get_metadata_profile(resolution)
        granule_list = self._get_metadata_granule_list()

        selected_bands = self.selector.get('bands')
        # TODO: navigate to resolution folder
        granule_list = self.filter_granules_by_resolution(granule_list, selected_bands, resolution)
        dataset = self._get_bands(product_node, granule_list, resolution)
        return dataset


class SentinelData(Data):

    def __init__(self, platformname, producttype, processinglevel):
        credentials = ('mluzu', 'aufklarung')
        odata_base_url = "https://apihub.copernicus.eu/apihub"
        self.open_search = SentinelOpenSearch(credentials, odata_base_url)
        self.navigator = S2MSINavigator(credentials, odata_base_url)

        self.products = None
        self.open_search.set_filter("platformname", platformname)
        self.open_search.set_filter("producttype", producttype)
        self.open_search.set_filter("processinglevel", processinglevel)

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
        self.open_search.set_filter('footprint', footprint)

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
        self.open_search.set_filter('beginposition', interval)
        self.open_search.set_filter('endposition', interval)

    def set_cloudcoverage_filter(self, minpercentage, maxpercentage):
        """
        Prefilter that can be applied by query.
        :param minpercentage:
        :param maxpercentage:
        """
        min_max = f'[{minpercentage} TO {maxpercentage}]'
        self.open_search.set_filter('cloudcoverpercentage', min_max)

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
        product_list = self.open_search.search()
        tile = self.navigator.get_dataset(product_list[0])
        return tile


class MSITile:
    def __init__(self, resolution, bands, profile, cloud_mask=None, defective_mask=None):
        self.resolution = resolution
        self.bands = bands
        self.profile = profile
        self.cloud_mask = None
        self.defective_mask = None

    def crop(self):
        pass
