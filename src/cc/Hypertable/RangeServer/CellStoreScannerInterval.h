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

#ifndef HYPERTABLE_CELLSTORESCANNERINTERVAL_H
#define HYPERTABLE_CELLSTORESCANNERINTERVAL_H

#include "Common/ByteString.h"
#include "Hypertable/Lib/Key.h"

namespace Hypertable {

  class CellStoreScannerInterval {
  public:
    CellStoreScannerInterval() : m_disk_read(0) { }
    virtual void forward() = 0;
    virtual bool get(Key &key, ByteString &value) = 0;
    virtual ~CellStoreScannerInterval() { }
    uint64_t get_disk_read() { return m_disk_read; }

  protected:
    struct BlockInfo {
      int64_t offset;
      int64_t zlength;
      const uint8_t *base;
      const uint8_t *end;
    };
    uint64_t m_disk_read;
  };

}

#endif // HYPERTABLE_CELLSTORESCANNERINTERVAL_H
