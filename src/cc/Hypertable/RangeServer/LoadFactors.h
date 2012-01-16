/** -*- c++ -*-
 * Copyright (C) 2007-2012 Hypertable, Inc.
 *
 * This file is part of Hypertable.
 *
 * Hypertable is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; version 3 of the
 * License.
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

#ifndef HYPERTABLE_LOADFACTORS_H
#define HYPERTABLE_LOADFACTORS_H

namespace Hypertable {

  class LoadFactors {
  public:
    
    LoadFactors() {
      reset();
    }

    void reset() {
      scans = 0;
      updates = 0;
      cells_scanned = 0;
      cells_written = 0;
      bytes_scanned = 0;
      bytes_written = 0;
      disk_bytes_read = 0;
    }

    uint64_t scans;
    uint64_t updates;
    uint64_t cells_scanned;
    uint64_t cells_written;
    uint64_t bytes_scanned;
    uint64_t bytes_written;
    uint64_t disk_bytes_read;
  };
}


#endif // HYPERTABLE_LOADFACTORS_H
