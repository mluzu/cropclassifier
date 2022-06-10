from .exceptions import SentinelAPIError, ReaderException
from requests import Session
from requests.exceptions import HTTPError
from io import StringIO
import xml.etree.ElementTree as ET


class SentinelClient:

    def __init__(self, credentials, odata_base_url="https://apihub.copernicus.eu/apihub", odata_path="/odata/v1"):
        self.session = Session()
        self.session.auth = credentials
        self.odata_base_url = odata_base_url
        self.odata_path = odata_path

    def get_link(self, link_url, stream=False):
        try:
            with self.session.get(link_url, stream=stream) as response:
                response.raise_for_status()
                return Reader(response.text)
        except HTTPError:
            raise SentinelAPIError("Failed request to SentinelApi", response)

    def get(self, query, stream=False):
        url = '{}{}{}'.format(self.odata_base_url, self.odata_path, query)
        try:
            with self.session.get(url, stream=stream) as response:
                response.raise_for_status()
                return Reader(response.text)
        except HTTPError:
            raise SentinelAPIError("Failed request to SentinelApi", response)

    def search(self, query, offset, page_size):
        url = '{}/search?start={}&rows={}&q={}'.format(self.odata_base_url, offset, page_size, query)
        try:
            with self.session.get(url, stream=False) as response:
                response.raise_for_status()
                return Reader(response.text)
        except HTTPError:
            raise SentinelAPIError("Failed request to SentinelApi", response)

    def stream(self, query):
        url = '{}{}{}'.format(self.odata_base_url, self.odata_path, query)
        try:
            with self.session.get(url, stream=True) as response:
                response.raise_for_status()
                return response.content
        except HTTPError:
            raise SentinelAPIError("Failed request to SentinelApi", response)


class Reader:
    def __init__(self, data, namespace=None):
        if isinstance(data, str):
            self.root, self.ns = Reader.parse_xml(data)
        elif isinstance(data, ET.Element):
            self.root, self.ns = data, Reader.node_namespaces(data, namespace)
        elif isinstance(data, ET.ElementTree):
            self.root, self.ns = data, Reader.node_namespaces(data, namespace)
        else:
            ReaderException("__init__", data)

    @staticmethod
    def node_namespaces(element, namespace):
        if namespace is not None:
            return namespace
        else:
            xml_text = ET.tostring(element)
            namespaces = dict([node for _, node in ET.iterparse(StringIO(xml_text.decode("utf-8")), events=['start-ns'])])
            return namespaces

    @staticmethod
    def parse_xml(xml_text):
        root = ET.fromstring(xml_text)
        namespaces = dict([node for _, node in ET.iterparse(StringIO(xml_text), events=['start-ns'])])
        return root, namespaces

    def get_value(self, xpath, formatter=str, use_namespace=True):
        if self.ns is None or not use_namespace:
            node = self.root.find(xpath)
        else:
            node = self.root.find(xpath, self.ns)

        if node is not None:
            return formatter(node.text)

    def get_node(self, xpath, use_namespace=True):
        if self.ns is None or not use_namespace:
            node = self.root.find(xpath)
        else:
            node = self.root.find(xpath, self.ns)

        if node is not None:
            return Reader(node, self.ns)

    def get_node_attr(self, xpath, attr, formatter=str, use_namespace=True):
        if self.ns is None or not use_namespace:
            node = self.root.find(xpath)
        else:
            node = self.root.find(xpath, self.ns)
        value = node.get(attr)
        return formatter(value)

    def get_all(self, xpath, use_namespace=True):
        if self.ns is None or not use_namespace:
            nodes = self.root.findall(xpath)
        else:
            nodes = self.root.findall(xpath, self.ns)

        if nodes is not None:
            return [Reader(node, self.ns) for node in nodes]

    def iterate(self, xpath):
        nodes = self.root.iter(xpath)
        if nodes is not None:
            for node in nodes:
                print(node)
            return [Reader(node) for node in nodes]

    
