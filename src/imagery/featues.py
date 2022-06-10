from shapely.geometry import shape
from rasterio.warp import transform_geom
from rasterio.mask import mask


class Feature:
    def __init__(self, collection):
        self.collection = collection
        self.shapes = Feature.geometry(collection)
        self._transformed = None

    @staticmethod
    def geometry(collection):
        return [
            shape(feature.get('geometry'))
            for feature in iter(collection)
        ]

    def bounds(self):
        return self.collection.bounds

    def transform_crs(self, crs):
        self._transformed = transform_geom(self.collection.crs, crs.data, self.shapes)

    def crop(self, array):
        cropped, out_transf = mask(array, self._transformed, crop=True)
        return cropped





