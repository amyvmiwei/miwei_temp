/*
 * Copyright (C) 2007-2015 Hypertable, Inc.
 *
 * This file is part of Hypertable.
 *
 * Hypertable is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; version 3 of the
 * License, or any later version.
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

#include <Common/Compat.h>

#include <Hypertable/Lib/Client.h>
#include <Hypertable/Lib/Future.h>

#include <Common/md5.h>
#include <Common/Time.h>
#include <Common/Usage.h>

#include <cstdlib>
#include <iostream>

extern "C" {
#include <unistd.h>
}

using namespace std;
using namespace Hypertable;

namespace {

  const char *schema =
    "<Schema>"
    "  <AccessGroup name=\"default\">"
    "    <ColumnFamily>"
    "      <Name>Field</Name>"
    "      <MaxVersions>1</MaxVersions>"
    "      <TimeOrder>DESC</TimeOrder>"
    "    </ColumnFamily>"
    "  </AccessGroup>"
    "</Schema>";

  const char *usage[] = {
    "usage: keyspec_test",
    "",
    "Validates key spec processing in RangeServer.",
    0
  };
}


int main(int argc, char **argv) {
  char *install_dir = getenv("INSTALL_DIR");

  HT_ASSERT(install_dir);

  if (argc > 2 ||
      (argc == 2 && (!strcmp(argv[1], "--help") || !strcmp(argv[1], "-?"))))
    Usage::dump_and_exit(usage);


  try {
    Client *hypertable = new Client(install_dir);
    NamespacePtr ns = hypertable->open_namespace("/");

    TablePtr table;
    KeySpec key;
    ScanSpec scan_spec;
    Cell cell;
    int64_t timestamp;

    /**
     * Load data
     */
    {
      ns->drop_table("KeyTest", true);
      ns->create_table("KeyTest", schema);

      table = ns->open_table("KeyTest");

      TableMutatorPtr mutator(table->create_mutator());

      key.row = "foo";
      key.row_len = strlen("foo");
      key.column_family = "Field";
      key.column_qualifier = 0;
      key.column_qualifier_len = 0;
      timestamp = get_ts64();
      key.timestamp = timestamp;

      mutator->set(key, "value 1", strlen("value 1"));
      mutator->flush();
    }

    {
      ScanSpec scan_spec;

      TableScannerPtr scanner(table->create_scanner(scan_spec));

      while (scanner->next(cell)) {
        // Verify that revision number assigned is greater than
        if ((int64_t)cell.revision < timestamp ||
            (cell.revision - timestamp) >= 5000000000LL) {
          HT_ERRORF("Supplied timestamp = %lld, returned revision = %lld",
                    (Lld)timestamp, (Lld)cell.revision);
          quick_exit(EXIT_FAILURE);
        }
      }
    }
  }
  catch (Exception &e) {
    HT_ERROR_OUT << e << HT_END;
    quick_exit(EXIT_FAILURE);
  }

  quick_exit(EXIT_SUCCESS);
}
