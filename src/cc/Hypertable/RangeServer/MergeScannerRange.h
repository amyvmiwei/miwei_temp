/* -*- c++ -*-
 * Copyright (C) 2007-2014 Hypertable, Inc.
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

/// @file
/// Declarations for MergeScannerRange.
/// This file contains type declarations for MergeScannerRange, a class used for
/// performing a scan over a range.

#ifndef Hypertable_RangeServer_MergeScannerRange_h
#define Hypertable_RangeServer_MergeScannerRange_h

#include <Hypertable/RangeServer/MergeScanner.h>
#include <Hypertable/RangeServer/IndexUpdater.h>

#include <Common/ByteString.h>
#include <Common/DynamicBuffer.h>

#include <queue>
#include <set>
#include <string>
#include <vector>

namespace Hypertable {

  /// @addtogroup RangeServer
  /// @{

  /// Performas a scan over a range.
  class MergeScannerRange : public MergeScanner {

  public:
    MergeScannerRange(const std::string &table_id, ScanContextPtr &scan_ctx);

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

    /// Index updater for <i>rebuild indices</i> scan
    IndexUpdaterPtr m_index_updater;
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
    int64_t       m_prev_timestamp;
    int32_t       m_prev_cf;
    bool          m_skip_this_row;
  };

  /// @}

} // namespace Hypertable

#endif // Hypertable_RangeServer_MergeScannerRange_h
