/**
 * Copyright (C) 2011 Hypertable, Inc.
 *
 * This file is part of Hypertable.
 *
 * Hypertable is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or any later version.
 *
 * Hypertable is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
 * 02110-1301, USA.
 */

#include "Common/Compat.h"
#include <iostream>
#include <map>
#include <cassert>
#include "Hypertable/Lib/Client.h"

using namespace Hypertable;

static int argc;
static char **argv;
static Client *ht_client;
static NamespacePtr ht_namespace;

int 
main(int _argc, char **_argv)
{
  argv = _argv;
  argc = _argc;

  String install;
  if (!getenv("INSTALL_DIR")) {
    printf("INSTALL_DIR environment variable is not set, using /opt/hypertable/current\n");
    install = "/opt/hypertable/current";
  }
  else
    install = getenv("INSTALL_DIR");
  install += "/conf/hypertable.cfg";

  try {
    ht_client = new Client(argv[0], install.c_str());
    ht_namespace = ht_client->open_namespace("/");

    TablePtr table = ht_namespace->open_table("LoadTest");

    // scan all keys with the index
    ScanSpecBuilder ssb;
    ssb.add_column("Field1");
    ssb.add_column_predicate("Field1", "", ColumnPredicate::PREFIX_MATCH, "");
    TableScanner *ts = table->create_scanner(ssb.get());
    Cell cell;
    int i = 0;
    while (ts->next(cell)) {
      if (++i > 10)
        break;
    }

    printf("deleting scanner\n");
    delete ts;

    ht_namespace = 0; // delete namespace before ht_client goes out of scope
    delete ht_client;
  }
  catch (Exception &ex) {
    std::cout << "caught exception: " << ex << std::endl;
    _exit(1);
  }
  _exit(0);
}
