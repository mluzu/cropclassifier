from imagery import Collection
import fiona
import time


shapefilePath = "/home/mluzu/cropclassifier/shapefile"


gral_lopez_collection = fiona.open(shapefilePath)
record = next(iter(gral_lopez_collection))

collection = Collection()
collection \
    .filter_bounds(gral_lopez_collection) \
    .filter_cloudcoverage(0, 10) \
    .filter_date('NOW-20DAYS', 'NOW')\
    .filter_bands('B01', 'B02', 'B03', 'B04', 'B05', 'B8A', 'B11', 'B12')

start_time = time.time()
dataset = collection.get()
print("--- %s seconds ---" % (time.time() - start_time))
