# -*- coding: utf-8 -*-
"""
Test helper to open the same file many times.
"""
import sys
import gcsfs

def run():
  if len(sys.argv) != 4:
    print 'usage: python -m gcsfs.tests.test_manyopens <project> <credentials_file|"cloud"> <text_file_on_gcs>'
    return
  project=sys.argv[1]
  credentials=sys.argv[2]
  file=sys.argv[3]
  print 'project: ' + project
  for i in range(2000):
    # Issue #12 only reproduces if I re-create the fs object every time.
    fs=gcsfs.GCSFileSystem(project=project, token=credentials)
    print 'attempt %s' % i
    with fs.open(file, 'rb') as o:
      line = o.readline()

if __name__ == '__main__':
  run()
