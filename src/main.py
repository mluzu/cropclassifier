from imagery import Collection, Feature
import fiona
import time


shapefilePath = "/home/mluzu/cropclassifier/shapefile"


gral_lopez_collection = fiona.open(shapefilePath)
feature = Feature(gral_lopez_collection)

collection = Collection(feature)
collection \
    .filter_bounds(gral_lopez_collection) \
    .filter_cloudcoverage(0, 10) \
    .filter_date('NOW-20DAYS', 'NOW')\
    .filter_bands('B01', 'B02', 'B03', 'B04', 'B05', 'B8A', 'B11', 'B12')

start_time = time.time()
cm, cp = collection.get()
print("--- %s seconds ---" % (time.time() - start_time))
