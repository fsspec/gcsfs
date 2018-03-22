#!/usr/bin/env python

import os
from setuptools import setup
import versioneer


setup(name='gcsfs',
      version=versioneer.get_version(),
      cmdclass=versioneer.get_cmdclass(),
      description='Convenient Filesystem interface over GCS',
      url='https://github.com/dask/gcsfs',
      maintainer='Martin Durant',
      maintainer_email='mdurant@continuum.io',
      license='BSD',
      keywords=['google-cloud-storage', 'gcloud', 'file-system'],
      packages=['gcsfs', 'gcsfs.cli'],
      install_requires=[open('requirements.txt').read().strip().split('\n')],
      long_description=(open('README.rst').read()
                        if os.path.exists('README.rst') else ''),
      extras_require={
          "gcsfuse": ["fusepy"]
      },
      entry_points='''
        [console_scripts]
        gcsfuse=gcsfs.cli.gcsfuse:main
      ''',
      zip_safe=False)
