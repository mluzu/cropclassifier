from requests import Session
from requests.exceptions import HTTPError
from .exceptions import SentinelAPIError
from .reader import Reader


class SentinelClient:

    def __init__(self, credentials, odata_base_url="https://apihub.copernicus.eu/apihub", odata_path="/odata/v1"):
        self.session = Session()
        self.session.auth = credentials
        self.odata_base_url = odata_base_url
        self.odata_path = odata_path

    def get(self, query, stream):
        url = '{}{}'.format(self.odata_base_url, query)
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

