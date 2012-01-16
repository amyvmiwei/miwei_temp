/** -*- c++ -*-
 * Copyright (C) 2007-2012 Hypertable, Inc.
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

#include "Common/Compat.h"
#include <cstdlib>
#include <iostream>

#include "Common/md5.h"
#include "Common/Usage.h"

#include "Hypertable/Lib/Client.h"

using namespace std;
using namespace Hypertable;

namespace {
  const char *usage[] = {
    "usage: scan_spec_test",
    "",
    "Runs basic tests of the ScanSpec class.",
    0
  };
}


int main(int argc, char **argv) {
  bool fired=false;

  if (argc !=1)
    Usage::dump_and_exit(usage);

  // not allowed: specifying cell offset and row offset
  try {
    ScanSpecBuilder ssb;
    ssb.set_row_offset(5);
    ssb.set_cell_offset(5);
  }
  catch (Exception &e) {
    if (e.code()!=Error::BAD_SCAN_SPEC) {
      std::cout << e << std::endl;
      _exit(1);
    }
    fired=true;
  }

  assert(fired==true);
  fired=false;

  // ... or vice versa
  try {
    ScanSpecBuilder ssb;
    ssb.set_cell_offset(5);
    ssb.set_row_offset(5);
  }
  catch (Exception &e) {
    if (e.code()!=Error::BAD_SCAN_SPEC) {
      std::cout << e << std::endl;
      _exit(1);
    }
    fired=true;
  }

  assert(fired==true);
  fired=false;

  _exit(0);
}
