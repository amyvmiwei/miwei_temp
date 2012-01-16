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

#ifndef HYPERTABLE_MERGESCANNERRANGE_H
#define HYPERTABLE_MERGESCANNERRANGE_H

#include <queue>
#include <string>
#include <vector>
#include <set>

#include "Common/ByteString.h"
#include "Common/DynamicBuffer.h"

#include "MergeScanner.h"

namespace Hypertable {

  class MergeScannerRange : public MergeScanner {

  public:
    MergeScannerRange(ScanContextPtr &scan_ctx);

    int32_t get_skipped_cells() {
      return m_cell_skipped;
    }

    int32_t get_skipped_rows() {
      return m_row_skipped;
    }

  protected:
    virtual bool do_get(Key &key, ByteString &value);
    virtual void do_initialize();
    virtual void do_forward();

  private:
    int32_t       m_cell_offset;
    int32_t       m_cell_skipped;
    int32_t       m_cell_count;
    int32_t       m_cell_limit;
    int32_t       m_row_offset;
    int32_t       m_row_skipped;
    int32_t       m_row_count;
    int32_t       m_row_limit;
    int32_t       m_cell_count_per_family;
    int32_t       m_cell_limit_per_family;
    DynamicBuffer m_prev_key;
    int32_t       m_prev_cf;
    bool          m_skip_this_row;
  };

} // namespace Hypertable

#endif // HYPERTABLE_MERGESCANNERRANGE_H
