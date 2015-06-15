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

/// @file
/// Declarations for CellStoreScanner.
/// This file contains the type declarations for CellStoreScanner, a class used
/// to scan over (query) some portion of a CellStore.

#ifndef Hypertable_RangeServer_CellStoreScanner_h
#define Hypertable_RangeServer_CellStoreScanner_h

#include <Hypertable/RangeServer/CellStore.h>
#include <Hypertable/RangeServer/CellListScanner.h>
#include <Hypertable/RangeServer/CellStoreScannerInterval.h>

#include <Common/DynamicBuffer.h>

#include <memory>

namespace Hypertable {

  /// @addtogroup RangeServer
  /// @{

  /// Provides the ability to scan over (query) a cell store.
  /// @tparam IndexT Type of block index
  template <typename IndexT>
  class CellStoreScanner : public CellListScanner {
  public:

    CellStoreScanner(CellStorePtr &&cellstore, ScanContext *scan_ctx,
                     IndexT *indexp=0);
    virtual ~CellStoreScanner();
    void forward() override;
    bool get(Key &key, ByteString &value) override;
    int64_t get_disk_read() override;

  private:
    CellStorePtr m_cellstore;
    std::unique_ptr<CellStoreScannerInterval> m_interval_scanners[3];
    size_t m_interval_index {};
    size_t m_interval_max {};
    DynamicBuffer m_key_buf;
    bool m_keys_only {};
    bool m_eos {};
    bool m_decrement_blockindex_refcount {};
  };

  /// @}

}

#endif // Hypertable_RangeServer_CellStoreScanner_h

