from setuptools import setup

setup(
   name='cropclassifier',
   version='0.0.1',
   description='Work in progress',
   author='Mariano Lzuriaga',
   author_email='luzuriagamariano@gmail.com',
   package_dir={'': 'imagery'},
   packages=['cropclassifier.imagery'],
   install_requires=['fiona', 'requests', 'fnmatch', 'numpy', 'rasterio'],
)
