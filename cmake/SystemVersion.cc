/** -*- C++ -*-
 * Copyright (C) 2007-2012 Hypertable, Inc.
 *
 * This file is part of Hypertable.
 *
 * Hypertable is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 3
 * of the License, or any later version.
 *
 * Hypertable is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Hypertable. If not, see <http://www.gnu.org/licenses/>
 */

#include <stdio.h>
#include <string.h>
#include <iostream>

extern "C" {
#include <sigar.h>
#include <sigar_format.h>
}

using namespace std;

int main() {
  sigar_sys_info_t s;
  sigar_t *_sigar = 0;
  if (sigar_open(&_sigar) != SIGAR_OK) {
    cerr << "sigar_open failure" << endl;
    return 1;
  }
  if (sigar_sys_info_get(_sigar, &s) != SIGAR_OK) {
    cerr << "sigar_sys_info_get failure" << endl;
    return 1;
  }
  cout << s.vendor << "_" << s.vendor_version << endl;
  return 0;
}
