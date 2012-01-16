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
#include <db_cxx.h>

int main() {
  int major, minor, patch;
  const char *version = DbEnv::version(&major, &minor, &patch);
  if (major != DB_VERSION_MAJOR || minor != DB_VERSION_MINOR ||
      patch != DB_VERSION_PATCH) {
    fprintf(stderr, "BerkeleyDB header/library mismatch:\n "
            "header: %s\nlibrary: %s\n", DB_VERSION_STRING, version);
    return 1;
  }
  printf("%d.%d.%d\n", major, minor, patch);
  return 0;
}
