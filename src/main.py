from imagery import Collection
import fiona
import time


shapefilePath = "/home/mluzu/cropclassifier/shapefile"


gral_lopez_collection = fiona.open(shapefilePath)
record = next(iter(gral_lopez_collection))

collection = Collection('Sentinel-2', 'S2MSI2A', 'Level-2A')
collection \
    .filter_bonds(gral_lopez_collection.bounds) \
    .filter_cloudcoverage(0, 10) \
    .filter_date('NOW-20DAYS', 'NOW')\
    .filter_bands('B01', 'B02', 'B03')

start_time = time.time()
prod = collection.read()
print("--- %s seconds ---" % (time.time() - start_time))
