from abc import ABC
from collections import OrderedDict, deque
from dataclasses import dataclass
from datetime import datetime
from .exceptions import SearchError


class SentinelProductList:

    def __init__(self, client, product_type, page_size=10, **filters):
        self.client = client
        self._product_type = product_type
        self._query_filters = filters

        self._items_number = 0
        self._consumed_items = 0
        self.page_size = page_size
        self.offset = -1
        self._current_page = deque()
        self._next_link = str

        self._product_builder = MSITileDescriptorBuilder()
        self._initialize()

    def __iter__(self):
        if self._items_number == 0:
            self.initialize()
        return self

    def __next__(self):
        """
        When only one items remains in the current_pages,then
        fetches the next page asynchronously and delivers last element
        in the current page.
        :return:
        """

        if self._consumed_items == self._items_number:
            raise StopIteration

        if len(self._current_page) == 1:
            self._next_page()

        item = self._current_page.pop(0)
        self.consumed_items += 1
        return self._product_builder.build(item)

    def __len__(self):
        if self._items_number == 0:
            self.initialize()
        return self._items_number

    def _initialize(self):
        try:
            query = self._build_search_query()
            page_reader = self.client.search(query, offset=0, page_size=self.page_size)
            self._items_number = page_reader.get_value('opensearch:totalResults', int)
            self._current_page.extend(page_reader.get_all("entry"))
            self.offset = self.page_size
            self._next_link = page_reader.get_node_attr("link[@rel='next']", 'href')
        except Exception as e:
            raise SearchError(e)

    def _build_search_query(self):

        self.add_query_filter("platformname", self._product_type.value[0])
        self.add_query_filter("producttype", self._product_type.value[1])
        self.add_query_filter("processinglevel", self._product_type.value[2])

        def partial(key, value):
            if isinstance(value, list):
                value = ' OR '.join(value)
                return f'{key}:({value})'
            else:
                return f'{key}:{value}'

        return ' AND '.join(
            [
                partial(key, value)
                for key, value in sorted(self._query_filters.items())
            ]
        )

    def _next_page(self):
        try:
            page_reader = self.client.get_link(self._next_link)
            self._current_page.extend(page_reader.get_all("entry"))
            self.offset += self.page_size
            self._next_link = page_reader.get_node_attr("link[@rel='next']", 'href')
        except Exception as e:
            raise SearchError(e)


class ProductDescriptorBuilder(ABC):
    def build(self, raw_list):
        pass

    def ingest(self, xml_content):
        pass


class MSITileDescriptorBuilder(ProductDescriptorBuilder):
    def __init__(self):
        self.tiles_list = []
        self.level1c = OrderedDict()
        self.level2a = OrderedDict()
        pass

    def build(self):
        """
        Implements build method in SearchParser abstract class.
        Creates a tile descriptor for each tile with the info retrieved during search.
        Compose level 1C and level 2A results into a single tile descriptor.
        :return: list of MSITileDescriptor
        """
        for name, level2a_item in self.level2a.items():
            lvl_1c_name = name.replace('2A', '1C')
            level2c_item = self.level1c.get(lvl_1c_name, None)
            if level2c_item is None:
                continue

            tile = S2MSITileDescriptor(
                name=level2a_item.get_value("title"),
                filename=level2a_item.get_value("str[@name='filename']"),
                level2A_uuid=level2a_item.get_value("str[@name='uuid']"),
                level1C_uuid=level2c_item.get_value("str[@name='uuid']"),
                level1C_filename=level2c_item.get_value("str[@name='filename']"),
                illuminationazimuthangle=level2a_item.get_value("double[@name='illuminationazimuthangle']", float),
                illuminationzenithangle=level2a_item.get_value("double[@name='illuminationzenithangle']", float),
                vegetationpercentage=level2a_item.get_value("double[@name='vegetationpercentage']", float),
                notvegetatedpercentage=level2a_item.get_value("double[@name='notvegetatedpercentage']", float),
                waterpercentage=level2a_item.get_value("double[@name='waterpercentage']", float),
                cloudcoverpercentage=level2a_item.get_value("double[@name='cloudcoverpercentage']", float),
                orbitnumber=level2a_item.get_value("int[@name='orbitnumber']", int),
                relativeorbitnumber=level2a_item.get_value("int[@name='relativeorbitnumber']", int),
                endposition=level2a_item.get_value("date[@name='endposition']", date_formatter),
            )
            self.tiles_list.append(tile)
        return self.tiles_list

    def ingest(self, reader):
        """
        Implements ingest method in SearchParser abstract class.
        Ingest the full-text search result from odata api page by page.
        Separates level 2A items from level 1C in order to make it easier
        the composition.
        :param reader:
        :return:
        """

        def get_short_name(i):
            title = i.get_value("title")
            title = title.rpartition('_')
            return title[0]

        leve2a_items = reader.get_all("entry/str[.='Level-2A']...")
        leve1c_items = reader.get_all("entry/str[.='Level-1C']...")

        for item in leve2a_items:
            self.level2a.update(
                {
                    get_short_name(item): item
                }
            )

        for item in leve1c_items:
            self.level1c.update(
                {
                    get_short_name(item): item
                }
            )


@dataclass
class S2MSITileDescriptor:
    name: str
    level2A_filename: str
    level2A_uuid: str
    level1C_filename: str
    level1C_uuid: str
    illuminationazimuthangle: float
    illuminationzenithangle: float
    vegetationpercentage: float
    notvegetatedpercentage: float
    waterpercentage: float
    cloudcoverpercentage: float
    orbitnumber: int
    relativeorbitnumber: int
    endposition: datetime

    def manifest_path_level2a(self):
        return "/Products('{}')/Nodes('{}')/Nodes('manifest.safe')/$value".format(
            self.level2A_uuid,
            self.level2A_filename
        )

    def manifest_path_level1c(self):
        return "/Products('{}')/Nodes('{}')/Nodes('manifest.safe')/$value".format(
            self.level1C_uuid,
            self.level1C_filename
        )


def date_formatter(str_time):
    return datetime.strptime(str_time, "%Y-%m-%dT%H:%M:%S.%fZ")


class MSITile:
    def __int__(self, tile_descriptor):
        self.tile_descriptor = tile_descriptor
        self.bands = []
        self.cloud_masks = []
