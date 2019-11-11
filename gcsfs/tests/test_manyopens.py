# -*- coding: utf-8 -*-
"""
Test helper to open the same file many times.

This is not a python unit test, but rather a standalone program that will open
a file repeatedly, to check whether a cloud storage transient error can
defeat gcsfs. This is to be run against real GCS, since we cannot capture
HTTP exceptions with VCR.

Ideally you should see nothing, just the attempt count go up until we're done.
"""
from __future__ import print_function
import sys
import gcsfs


def run():
    if len(sys.argv) != 4:
        print(
            "usage: python -m gcsfs.tests.test_manyopens <project> "
            '<credentials_file|"cloud"> <text_file_on_gcs>'
        )
        return
    project = sys.argv[1]
    credentials = sys.argv[2]
    file = sys.argv[3]
    print("project: " + project)
    for i in range(2000):
        # Issue #12 only reproduces if I re-create the fs object every time.
        fs = gcsfs.GCSFileSystem(project=project, token=credentials)
        print("attempt %s" % i)
        with fs.open(file, "rb") as o:
            o.readline()


if __name__ == "__main__":
    run()
