from .sentineldata import SentinelData


class Collection:
    def __init__(self, platformname, producttype, processinglevel) -> None:
        self.data_source = SentinelData(platformname, producttype, processinglevel)

    def filter_bonds(self, rectangle):
        self.data_source.set_bonds_filter(rectangle)
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

    def read(self, count=1):
        image = self.data_source.read(count)
        return image


  