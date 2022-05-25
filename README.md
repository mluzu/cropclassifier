# Cropclassifier
A crop classifier as a web service

## First Steps

### Environment setup
- pyenv install 3.8.8
- pyenv global 3.8.8
- pip install pipenv
- pipenv --python 3.8.8 install

### Install GDAL
- sudo apt-get install libgdal-dev
- export CPLUS_INCLUDE_PATH=/usr/include/gdal
- export C_INCLUDE_PATH=/usr/include/gdal

### Install project dependencies
La versión actual de rasterio (1.2) es compatible con Python 3.6 a 3.9, numpy >= 1.15 y con GDAL 2.3 a 3.2.

- pipenv --rm (ejecutar antes en caso de que al a primera arroje el error ERROR:: --system is intended to be used for pre-existing Pipfile installation, not installation of specific packages. Aborting.)
- pipenv shell (activa entorno)
- pipenv install
