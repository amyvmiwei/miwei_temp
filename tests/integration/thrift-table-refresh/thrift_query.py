#!/usr/bin/env python

import sys
import time
from hypertable.thriftclient import *
from hyperthrift.gen.ttypes import *

if (len(sys.argv) < 2):
  print sys.argv[0], "<table>"
  sys.exit(1);

try:
  client = ThriftClient("localhost", 15867)

  namespace = client.open_namespace("/")

  scanner = client.open_scanner(namespace, sys.argv[1],
                                ScanSpec(None, None, None, 1));

  while True:
    cells = client.next_cells(scanner)
    for cell in cells:
      print cell.key.row, cell.key.column_family, cell.value
    if (len(cells) == 0):
      break

  client.close_namespace(namespace)

except ClientException, e:
  print '%s' % (e.message)
