.. ESGF Data Downloader documentation master file, created by
   sphinx-quickstart on Thu Nov  7 14:59:12 2013.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

Welcome to ESGF Data Downloader's documentation!
================================================

The ESGF data downloader package is designed for downloading climate model data from the Earth Systems Grid Federation system. It provides a fully featured set of tools to query for data to download and download that data.

This tool is currently in development. It works well - for us. If it doesn't work well for you, please contact me and let me know.

Getting Started
===============


The package can be downloaded via ``pip`` or ``easy_install``::

  $ pip install esgf_download
  $ easy_install esgf_download

You can also download the tarball from http://pypi.python.org/pypi/esgf_download and install manually as follows::

  $ tar zxf esgf_download-*.tar.gz
  $ cd esgf_download-*
  $ python setup.py install

If you want to follow the latest code and/or make contributions the source code is available on github at https://github.com/bronaugh/esgf_download

Once installed, you may use the ``esgf_add_downloads.py`` tool to add downloads based on query parameters, and the ``esgf_fetch_downloads.py`` tool to retrieve those downloads. See the `recipes` for examples.


Contents
========

.. toctree::
   :maxdepth: 2

   esgf_download
   recipes

Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`

