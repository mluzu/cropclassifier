from abc import ABC, abstractmethod


class Data(ABC):
    """
    Every class interfacing with a satellite should implement this abstract
    class in order to enforce the expected behaviour in Collection class.
    """

    @abstractmethod
    def set_bounds_filter(self, bounds):
        pass

    @abstractmethod
    def set_date_filter(self, begin, end):
        pass

    @abstractmethod
    def set_bands_filter(self, *bands):
        pass

    @abstractmethod
    def set_cloudcoverage_filter(self, minpercentage, maxpercentage):
        pass

    @abstractmethod
    def fetch_products(self):
        pass

    @abstractmethod
    def read(self, count):
        pass
