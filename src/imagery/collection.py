from .sentinel import SentinelData
from .sentinel import ProductType


class Collection:
    def __init__(self, feature) -> None:
        self.data_source = SentinelData(ProductType.S2MSI, feature)

    def filter_bounds(self, geom):
        self.data_source.set_bounds_filter(geom.bounds)
        return self

    def filter_date(self, begin, end):
        self.data_source.set_date_filter(begin, end)
        return self

    def filter_bands(self, *bands):
        self.data_source.set_bands_filter(*bands)
        return self

    def filter_cloudcoverage(self,  minpercentage, maxpercentage):
        self.data_source.set_cloudcoverage_filter(minpercentage, maxpercentage)
        return self

    def get(self):
        image = self.data_source.get()
        return image


  