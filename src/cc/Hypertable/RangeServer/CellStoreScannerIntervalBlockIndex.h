/* -*- c++ -*-
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

/// @file
/// Declarations for CellStoreScannerIntervalBlockIndex.
/// This file contains the type declarations for
/// CellStoreScannerIntervalBlockIndex, a class used to scan over a portion of a
/// CellStore using its block index.

#ifndef HYPERTABLE_CELLSTORESCANNERINTERVALBLOCKINDEX_H
#define HYPERTABLE_CELLSTORESCANNERINTERVALBLOCKINDEX_H

#include <Hypertable/RangeServer/CellStore.h>
#include <Hypertable/RangeServer/CellStoreScannerInterval.h>
#include <Hypertable/RangeServer/ScanContext.h>

#include <Common/DynamicBuffer.h>

namespace Hypertable {

  class BlockCompressionCodec;
  class CellStore;

  /// @addtogroup RangeServer
  /// @{

  /// Provides the ability to scan over a portion of a cell store using its block index.
  /// @tparam IndexT Type of block index
  template <typename IndexT>
  class CellStoreScannerIntervalBlockIndex : public CellStoreScannerInterval {
  public:

    typedef typename IndexT::iterator IndexIteratorT;

    CellStoreScannerIntervalBlockIndex(CellStore *cellstore, IndexT *index,
                                       SerializedKey start_key, SerializedKey end_key, ScanContextPtr &scan_ctx);
    virtual ~CellStoreScannerIntervalBlockIndex();
    virtual void forward();
    virtual bool get(Key &key, ByteString &value);

  private:

    bool fetch_next_block(bool eob=false);

    CellStorePtr          m_cellstore;
    IndexT               *m_index;
    IndexIteratorT        m_iter;
    BlockInfo             m_block;
    Key                   m_key;
    SerializedKey         m_cur_key;
    ByteString            m_cur_value;
    SerializedKey         m_start_key;
    SerializedKey         m_end_key;
    const char *          m_end_row;
    DynamicBuffer         m_key_buf;
    BlockCompressionCodec *m_zcodec;
    KeyDecompressor      *m_key_decompressor;
    int32_t               m_fd;
    bool                  m_cached;
    bool                  m_check_for_range_end;
    int                   m_file_id;
    ScanContextPtr        m_scan_ctx;
    ScanContext::CstrRowSet& m_rowset;
  };

  /// @}
}

#endif // HYPERTABLE_CELLSTORESCANNERINTERVALBLOCKINDEX_H
