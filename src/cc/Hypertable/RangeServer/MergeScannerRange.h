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

#include <Hypertable/RangeServer/MergeScannerAccessGroup.h>
#include <Hypertable/RangeServer/IndexUpdater.h>

#include <Common/ByteString.h>
#include <Common/DynamicBuffer.h>

#include <memory>
#include <queue>
#include <set>
#include <string>
#include <vector>

namespace Hypertable {

  /// @addtogroup RangeServer
  /// @{

  /// Performs a scan over a range.
  class MergeScannerRange {

  public:
    MergeScannerRange(const std::string &table_id, ScanContextPtr &scan_ctx);

    /// Destructor.
    /// Destroys all scanners in #m_scanners.
    virtual ~MergeScannerRange();

    void add_scanner(MergeScannerAccessGroup *scanner) {
      m_scanners.push_back(scanner);
    }

    void forward();

    bool get(Key &key, ByteString &value);

    int32_t get_skipped_cells() { return m_cell_skipped; }

    int32_t get_skipped_rows() { return m_row_skipped; }

    /// Returns number of cells input.
    /// Calls MergeScannerAccessGroup::get_input_cells() on each scanner in
    /// #m_scanners and returns the aggregated result.
    /// @return number of cells input.
    int64_t get_input_cells();

    /// Returns number of cells output.
    /// Returns number of cells returned via calls to get().
    /// @return Number of cells output.
    int64_t get_output_cells() { return m_cells_output; }

    /// Returns number of bytes input.
    /// Calls MergeScannerAccessGroup::get_input_bytes() on each scanner in
    /// #m_scanners and returns the aggregated result.
    /// @return number of bytes input.
    int64_t get_input_bytes();

    /// Returns number of bytes output.
    /// Returns number of bytes returned via calls to get().
    /// @return Number of cells output.
    int64_t get_output_bytes() { return m_bytes_output; }

    int64_t get_disk_read();

    ScanContext *scan_context() { return m_scan_context.get(); }

  private:

    void initialize();

    struct ScannerState {
      MergeScannerAccessGroup *scanner;
      Key key;
      ByteString value;
    };

    struct LtScannerState {
      bool operator()(const ScannerState &ss1, const ScannerState &ss2) const {
        return ss1.key.serial > ss2.key.serial;
      }
    };

    std::vector<MergeScannerAccessGroup *>  m_scanners;
    std::priority_queue<ScannerState, std::vector<ScannerState>,
                        LtScannerState> m_queue;

    /// Scan context
    ScanContextPtr m_scan_context;

    /// Index updater for <i>rebuild indices</i> scan
    IndexUpdaterPtr m_index_updater;

    /// Flag indicating scan is finished
    bool m_done {};

    /// Flag indicating if scan has been initialized
    bool m_initialized {};

    bool m_skip_this_row {};

    int32_t m_cell_offset {};
    int32_t m_cell_skipped {};
    int32_t m_cell_count {};
    int32_t m_cell_limit {};
    int32_t m_row_offset {};
    int32_t m_row_skipped {};
    int32_t m_row_count {};
    int32_t m_row_limit {};
    int32_t m_cell_count_per_family {};
    int32_t m_cell_limit_per_family {};
    int32_t m_prev_cf {-1};
    int64_t m_prev_timestamp {TIMESTAMP_NULL};
    int64_t m_bytes_output {};
    int64_t m_cells_output {};
    DynamicBuffer m_prev_key;

  };

  /// Smart pointer to MergeScannerRange
  typedef std::shared_ptr<MergeScannerRange> MergeScannerRangePtr;

  /// @}

} // namespace Hypertable

#endif // Hypertable_RangeServer_MergeScannerRange_h
