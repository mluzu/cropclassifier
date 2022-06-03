from requests import Session
from requests.exceptions import HTTPError
from src.imagery.sentinel.exceptions import SentinelAPIError


class SentinelClient:

    def __init__(self, credentials, odata_base_url="https://apihub.copernicus.eu/apihub"):
        self.session = Session()
        self.session.auth = credentials
        self.odata_base_url = odata_base_url

    def api_call(self, query, stream, as_text=True):
        url = '{}{}'.format(self.odata_base_url, query)
        try:
            with self.session.get(url, stream=stream) as response:
                response.raise_for_status()
                if as_text:
                    return response.text
                else:
                    return response.content
        except HTTPError:
            raise SentinelAPIError("Failed request to SentinelApi", response)
