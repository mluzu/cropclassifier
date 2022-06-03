import re
import fnmatch
from io import StringIO
import xml.etree.ElementTree as xml


def namespace(element):
    m = re.match(r'\{.*\}', element.tag)
    return m.group(0) if m else ''


def parse_xml(xml_text):
    root = xml.fromstring(xml_text)
    namespaces = dict([node for _, node in xml.iterparse(StringIO(xml_text), events=['start-ns'])])
    return root, namespaces


class S2MSINavigator:

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
            query = "{}{}/Nodes('{}')/Nodes('{}')/Nodes('{}')/Nodes('{}')/Nodes('{}.jp2')/$value" \
                .format(self.odata_path, product_node, granule, identifier, img_folder, res_folder, file_name)
            image_bytes = self.api_call(query, stream=True)
            mem_file = MemoryFile(image_bytes)
            return mem_file.open(**profile)

    def get_dataset(self, product_node):
        self._get_manifest(product_node)
        self._get_product_metadata(product_node)
        granule_title = self._get_granule_title(product_node)
        self._get_granule_metadata(product_node, granule_title)

        resolution = self.selector.get('resolution')
        granule_list = self._get_metadata_granule_list()

        selected_bands = self.selector.get('bands')
        # TODO: navigate to resolution folder
        granule_list = self.filter_granules_by_resolution(granule_list, selected_bands, resolution)
        dataset = self._get_bands(product_node, granule_list, resolution)
        return dataset
