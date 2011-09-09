/** -*- c++ -*-
 * Copyright (C) 2008 Doug Judd (Zvents, Inc.)
 *
 * This file is part of Hypertable.
 *
 * Hypertable is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; version 2 of the
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

  protected:
    virtual bool do_get(Key &key, ByteString &value);
    virtual void do_initialize();
    virtual void do_forward();

  private:
    int32_t       m_cell_count;
    int32_t       m_cell_limit;
    int32_t       m_row_count;
    int32_t       m_row_limit;
    int32_t       m_cell_count_per_family;
    int32_t       m_cell_limit_per_family;
    DynamicBuffer m_prev_key;
    int32_t       m_prev_cf;
  };

} // namespace Hypertable

#endif // HYPERTABLE_MERGESCANNERRANGE_H
