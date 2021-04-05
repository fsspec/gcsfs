#!/usr/bin/env python

import os
from setuptools import setup
import versioneer


setup(
    name="gcsfs",
    version=versioneer.get_version(),
    cmdclass=versioneer.get_cmdclass(),
    description="Convenient Filesystem interface over GCS",
    url="https://github.com/dask/gcsfs",
    maintainer="Martin Durant",
    maintainer_email="mdurant@anaconda.com",
    license="BSD",
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: BSD License",
        "Operating System :: OS Independent",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
    ],
    keywords=["google-cloud-storage", "gcloud", "file-system"],
    packages=["gcsfs", "gcsfs.cli"],
    install_requires=[open("requirements.txt").read().strip().split("\n")],
    long_description=(
        open("README.rst").read() if os.path.exists("README.rst") else ""
    ),
    extras_require={"gcsfuse": ["fusepy"], "crc": ["crcmod"]},
    python_requires=">=3.6",
    zip_safe=False,
)
