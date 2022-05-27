from setuptools import setup, find_packages

setup(
   name='cropclassifier',
   version='0.0.1',
   description='Work in progress',
   author='Mariano Lzuriaga',
   author_email='luzuriagamariano@gmail.com',
   package_dir={"": "src"},
   packages=find_packages(where="src"),
   install_requires=['fiona', 'requests', 'numpy', 'rasterio'],
)
