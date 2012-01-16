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

#ifndef HYPERTABLE_CELLLISTSCANNER_H
#define HYPERTABLE_CELLLISTSCANNER_H

#include "Common/ByteString.h"
#include "Common/DynamicBuffer.h"
#include "Common/ReferenceCount.h"

#include "ScanContext.h"


namespace Hypertable {

  class CellListScanner : public ReferenceCount {
  public:
    CellListScanner() : m_disk_read(0) { return; }
    CellListScanner(ScanContextPtr &scan_ctx) : m_disk_read(0), m_scan_context_ptr(scan_ctx) { }
    virtual ~CellListScanner() { return; }

    virtual void forward() = 0;
    virtual bool get(Key &key, ByteString &value) = 0;

    ScanContext *scan_context() { return m_scan_context_ptr.get(); }

    virtual uint64_t get_disk_read() = 0;
    void add_disk_read(uint64_t amount) { m_disk_read += amount; }

  protected:
    uint64_t m_disk_read;
    ScanContextPtr m_scan_context_ptr;
  };

  typedef boost::intrusive_ptr<CellListScanner> CellListScannerPtr;

}

#endif // HYPERTABLE_CELLLISTSCANNER_H

