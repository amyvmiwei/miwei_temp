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
#include <Common/Error.h>
#include <Common/HostSpecification.h>
#include <Common/String.h>

#include <iostream>
#include <string>
#include <vector>

using namespace Hypertable;
using namespace std;

namespace {
  void dump_usage_and_exit() {
    cout << endl;
    cout << "usage: ht_expand_hostspec <host-specification>" << endl;
    cout << endl;
    cout << "This program expands a host specification into a" << endl;
    cout << "space-separated list of host names." << endl;
    cout << endl;
    quick_exit(EXIT_FAILURE);
  }
}

/// @defgroup expand_hostspec expand_hostspec
/// @ingroup Tools
/// Host specification expansion tool.

int main(int argc, char **argv) {

  if (argc < 2)
    dump_usage_and_exit();

  string spec;
  for (int i=1; i<argc; i++)
    spec.append(format(" (%s)", argv[i]));

  try {
    vector<string> hosts = HostSpecification(spec);
    bool first = true;
    for (auto & host : hosts) {
      if (!first)
        cout << " ";
      cout << host;
      first = false;
    }
    cout << endl;
  }
  catch (Exception &e) {
    cout << Error::get_text(e.code()) << " - " << e.what() << endl;
    quick_exit(EXIT_FAILURE);
  }

  quick_exit(EXIT_SUCCESS);
}
