/**
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
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
 * 02110-1301, USA.
 */

#include "Common/Compat.h"
#include "Common/TimeInline.h"

#include "iostream"

using namespace Hypertable;
using namespace std;


#define VERIFY_OUT_OF_RANGE(_timestr_) do { \
    try { parse_ts(_timestr_); } \
    catch (std::exception &e) { break; } \
    cout << _timestr_ << " parsed successfullly, but is invalid" << endl; \
    exit(0); \
  } while (0)



int main(int ac, char *av[]) {
  int64_t ts;

  // Check valid time lower and upper bounds
  try {
    ts = parse_ts("2011-01-01 00:00:00");
    ts = parse_ts("2011-12-31 23:59:59");
    (void)ts; // avoid warning because ts is set but never used
  }
  catch (std::exception &e) {
    cout << e.what() << endl;
    return 1;
  }

  VERIFY_OUT_OF_RANGE("2011-00-15 12:30:30");
  VERIFY_OUT_OF_RANGE("2011-13-15 12:30:30");
  VERIFY_OUT_OF_RANGE("2011-06-00 12:30:30");
  VERIFY_OUT_OF_RANGE("2011-06-32 12:30:30");
  VERIFY_OUT_OF_RANGE("2011-06-15 24:30:30");
  VERIFY_OUT_OF_RANGE("2011-06-15 12:60:30");
  VERIFY_OUT_OF_RANGE("2011-06-15 12:30:60");

  return 0;
}
