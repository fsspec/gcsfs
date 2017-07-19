#!/usr/bin/env python

import os
from setuptools import setup

setup(name='gcsfs',
      version='0.0.2',
      description='Convenient Filesystem interface over GCS',
      url='https://github.com/dask/gcsfs',
      maintainer='Martin Durant',
      maintainer_email='mdurant@continuum.io',
      license='BSD',
      keywords=['google-cloud-storage', 'gcloud', 'file-system'],
      packages=['gcsfs'],
      install_requires=[open('requirements.txt').read().strip().split('\n')],
      long_description=(open('README.rst').read() if os.path.exists('README.rst')
                        else ''),
      zip_safe=False)
