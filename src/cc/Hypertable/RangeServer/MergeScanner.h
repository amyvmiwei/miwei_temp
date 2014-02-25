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

#ifndef HYPERTABLE_MERGESCANNER_H
#define HYPERTABLE_MERGESCANNER_H

#include <queue>
#include <string>
#include <vector>
#include <set>

#include "Common/ByteString.h"
#include "Common/DynamicBuffer.h"

#include "CellListScanner.h"
#include "CellStoreReleaseCallback.h"


namespace Hypertable {

  class MergeScanner : public CellListScanner {

  public:

    enum Flags {
      RETURN_DELETES = 0x00000001,
      IS_COMPACTION = 0x00000002,
      ACCUMULATE_COUNTERS = 0x00000004
    };

    struct ScannerState {
      CellListScanner *scanner;
      Key key;
      ByteString value;
    };

    struct LtScannerState {
      bool operator()(const ScannerState &ss1, const ScannerState &ss2) const {
        return ss1.key.serial > ss2.key.serial;
      }
    };

    MergeScanner(ScanContextPtr &scan_ctx, uint32_t flags=0);

    virtual ~MergeScanner();

    virtual void forward();

    virtual bool get(Key &key, ByteString &value);

    uint32_t get_flags() { return m_flags; }

    void add_scanner(CellListScanner *scanner);

    void install_release_callback(CellStoreReleaseCallback &cb) {
      m_release_callback = cb;
    }

    void io_add_input_cell(uint64_t cur_bytes) {
      m_bytes_input += cur_bytes;
      m_cells_input++;
    }

    void io_add_output_cell(uint64_t cur_bytes) {
      m_bytes_output += cur_bytes;
      m_cells_output++;
    }

    void get_io_accounting_data(uint64_t *inbytesp, uint64_t *outbytesp,
                                uint64_t *incellsp=0, uint64_t *outcellsp=0) {
      *inbytesp = m_bytes_input;
      *outbytesp = m_bytes_output;
      if (incellsp)
        *incellsp = m_cells_input;
      if (outcellsp)
        *outcellsp = m_cells_output;
    }

    virtual uint64_t get_disk_read();

  protected:
    void initialize();
    virtual bool do_get(Key &key, ByteString &value) = 0;
    virtual void do_initialize() = 0;
    virtual void do_forward() = 0;

    uint32_t m_flags {};
    bool m_done {};
    bool m_initialized {};

    std::vector<CellListScanner *>  m_scanners;
    std::priority_queue<ScannerState, std::vector<ScannerState>,
        LtScannerState> m_queue;

    CellStoreReleaseCallback m_release_callback;

  private:
    uint64_t m_bytes_input {};
    uint64_t m_bytes_output {};
    uint64_t m_cells_input {};
    uint64_t m_cells_output {};
  };

  typedef boost::intrusive_ptr<MergeScanner> MergeScannerPtr;


} // namespace Hypertable

#endif // HYPERTABLE_MERGESCANNER_H
