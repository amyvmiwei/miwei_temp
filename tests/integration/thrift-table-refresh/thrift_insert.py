#!/usr/bin/env python

import sys
import time
from hypertable.thriftclient import *
from hyperthrift.gen.ttypes import *

if (len(sys.argv) < 4):
  print sys.argv[0], "<table> <row-key> <column>"
  sys.exit(1);

try:
  client = ThriftClient("localhost", 38080)

  namespace = client.open_namespace("/")

  mutator = client.open_mutator(namespace, sys.argv[1], 0, 0)

  client.set_cell(mutator, Cell(Key(sys.argv[2], sys.argv[3], None), "thrift_insert.py"))
  client.flush_mutator(mutator);

  client.close_namespace(namespace)

except ClientException, e:
  print '%s' % (e.message)
