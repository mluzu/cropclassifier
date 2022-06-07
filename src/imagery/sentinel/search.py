from abc import ABC
from collections import OrderedDict
from dataclasses import dataclass
from datetime import datetime
from .exceptions import SearchError


class SentinelProductList:

    def __init__(self, client, product_type, page_size=10):
        self._index = -1
        self._items_number = 0
        self._products_list = []
        self.client = client
        self.list_builder = MSISearchParser()
        self._product_type = product_type
        self.page_size = page_size
        self._query_filters = dict()

    def __iter__(self):
        if self._items_number == 0:
            self._build()
        return self

    def __next__(self):
        if self._items_number == 0:
            self._build()

        self._index += 1
        if self._index > self._items_number:
            raise StopIteration
        return self._products_list[self._index]

    def __getitem__(self, item):
        if self._items_number == 0:
            self._build()
        return self._products_list[item]

    def __len__(self):
        if self._items_number == 0:
            self._build()
        return self._items_number

    def add_query_filter(self, param, value):
        self._query_filters.update({param: value})

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

    def _first_page(self):
        query = self._build_search_query()
        return self.client.search(query, offset=0, page_size=self.page_size)

    def _next_page(self, page_reader):
        next_url = page_reader.get_node_attr("link[@rel='next']", 'href')
        return self.client.get_link(next_url)

    def _build(self):
        offset = 0
        try:
            page_reader = self._first_page()
            total = page_reader.get_value('opensearch:totalResults', int)

            while True:
                self.list_builder.ingest(page_reader)
                offset = offset + self.page_size
                if offset >= total:
                    break
                page_reader = self._next_page(page_reader)

        except Exception as e:
            raise SearchError(e)

        self._products_list = self.list_builder.build()
        self._items_number = len(self._products_list)


class SearchParser(ABC):
    def build(self, raw_list):
        pass

    def ingest(self, xml_content):
        pass


class MSISearchParser(SearchParser):
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
        for name, entry in self.level2a.items():
            name = name.replace('2A', '1C')
            lvl1c = self.level1c.get(name, None)
            if lvl1c is None:
                continue

            tile = S2MSITileDescriptor(
                name=entry.get_value("title"),
                level2A_uuid=entry.get_value("str[@name='uuid']"),
                level1C_uuid=lvl1c.get_value("str[@name='uuid']"),
                level1C_tile=lvl1c.get_value("title"),
                illuminationazimuthangle=entry.get_value("double[@name='illuminationazimuthangle']", float),
                illuminationzenithangle=entry.get_value("double[@name='illuminationzenithangle']", float),
                vegetationpercentage=entry.get_value("double[@name='vegetationpercentage']", float),
                notvegetatedpercentage=entry.get_value("double[@name='notvegetatedpercentage']", float),
                waterpercentage=entry.get_value("double[@name='waterpercentage']", float),
                cloudcoverpercentage=entry.get_value("double[@name='cloudcoverpercentage']", float),
                orbitnumber=entry.get_value("int[@name='orbitnumber']", int),
                relativeorbitnumber=entry.get_value("int[@name='relativeorbitnumber']", int),
                endposition=entry.get_value("date[@name='endposition']", date_formatter),
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
    level2A_uuid: str
    level1C_uuid: str
    level1C_tile: str
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
        return "/Products('{}')/Nodes('{}.SAFE')/Nodes('manifest.safe')/$value".format(
            self.level2A_uuid,
            self.name
        )

    def manifest_path_level1c(self):
        return "/Products('{}')/Nodes('{}.SAFE')/Nodes('manifest.safe')/$value".format(
            self.level1C_uuid,
            self.level1C_tile
        )

    def granule_path(self):
        return


def date_formatter(str_time):
    return datetime.strptime(str_time, "%Y-%m-%dT%H:%M:%S.%fZ")
