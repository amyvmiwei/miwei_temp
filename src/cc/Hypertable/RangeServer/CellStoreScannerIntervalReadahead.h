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
/// Declarations for CellStoreScannerIntervalReadahead.
/// This file contains the type declarations for
/// CellStoreScannerIntervalReadahead, a class that provides the ability to
/// efficiently scan over (query) a portion of a CellStore using readahead.

#ifndef Hypertable_RangeServer_CellStoreScannerIntervalReadahead_h
#define Hypertable_RangeServer_CellStoreScannerIntervalReadahead_h

#include <Hypertable/RangeServer/CellStore.h>
#include <Hypertable/RangeServer/CellStoreScannerInterval.h>
#include <Hypertable/RangeServer/ScanContext.h>

#include <Common/DynamicBuffer.h>

namespace Hypertable {

  class BlockCompressionCodec;

  /// @addtogroup RangeServer
  /// @{

  /// Provides ability to efficiently scan over a portion of a cell store.
  /// @tparam IndexT Type of block index
  template <typename IndexT>
  class CellStoreScannerIntervalReadahead : public CellStoreScannerInterval {
  public:

    typedef typename IndexT::iterator IndexIteratorT;

    CellStoreScannerIntervalReadahead(CellStorePtr &cellstore, IndexT *index,
                                      SerializedKey start_key,SerializedKey end_key, ScanContext *scan_ctx);
    virtual ~CellStoreScannerIntervalReadahead();
    virtual void forward();
    virtual bool get(Key &key, ByteString &value);

  private:

    bool fetch_next_block_readahead(bool eob=false);

    CellStorePtr           m_cellstore;
    BlockInfo              m_block;
    Key                    m_key;
    SerializedKey          m_end_key;
    ByteString             m_cur_value;
    BlockCompressionCodec *m_zcodec {};
    KeyDecompressor       *m_key_decompressor {};
    int32_t                m_fd {-1};
    int64_t                m_offset {};
    int64_t                m_end_offset {};
    bool                   m_check_for_range_end {};
    bool                   m_eos {};
    ScanContext           *m_scan_ctx {};
    uint32_t               m_oflags {};

  };

  /// @}

}

#endif // Hypertable_RangeServer_CellStoreScannerIntervalReadahead_h
