/* -*- c++ -*-
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

#ifndef Hypertable_RangeServer_CellListScanner_h
#define Hypertable_RangeServer_CellListScanner_h

#include "ScanContext.h"

#include <Common/ByteString.h>
#include <Common/DynamicBuffer.h>

#include <memory>

namespace Hypertable {

  class CellListScanner {
  public:
    CellListScanner() { }
    CellListScanner(ScanContext *scan_ctx) : m_scan_context_ptr(scan_ctx) { }
    virtual ~CellListScanner() { return; }

    virtual void forward() = 0;
    virtual bool get(Key &key, ByteString &value) = 0;

    ScanContext *scan_context() { return m_scan_context_ptr; }

    virtual int64_t get_disk_read() = 0;
    void add_disk_read(int64_t amount) { m_disk_read += amount; }

  protected:
    uint64_t m_disk_read {};
    ScanContext *m_scan_context_ptr {};
  };

  typedef std::shared_ptr<CellListScanner> CellListScannerPtr;

}

#endif // Hypertable_RangeServer_CellListScanner_h

