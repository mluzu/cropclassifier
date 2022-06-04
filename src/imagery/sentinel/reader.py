import re
import fnmatch
from io import StringIO
import xml.etree.ElementTree as ET


class Reader:
    def __init__(self, raw_content):
        self.root, self.ns = Reader.parse_xml(raw_content)

    @staticmethod
    def parse_xml(xml_text):
        root = ET.fromstring(xml_text)
        namespaces = dict([node for _, node in ET.iterparse(StringIO(xml_text), events=['start-ns'])])
        return root, namespaces

    def product_list(self):
        items_nodes = self.root.findall('entry', self.ns)
        product_list = []
        for node in items_nodes:
            product_list.append(
                f"/Products('{node.find('id', self.ns).text}')/Nodes('{node.find('title', self.ns).text}.SAFE')"
            )
        return product_list

    def next_page(self):
        pass

    def search_total(self):
        return int(self.root.find('opensearch:totalResults', self.ns).text)

    def iter(self, path):
        items = self.root.findall(path, self.ns)
        if items is None:
            raise Exception
        return items
