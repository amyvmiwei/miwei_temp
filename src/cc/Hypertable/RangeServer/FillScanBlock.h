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
/// Declaration for FillScanBlock.
/// This file contains the type declaration for FillScanBlock, a function for
/// filling a block of scan (query) results to be returned back to the client.


#ifndef Hypertable_RangeServer_FillScanBlock_h
#define Hypertable_RangeServer_FillScanBlock_h

#include <Hypertable/RangeServer/CellListScanner.h>

#include <Common/DynamicBuffer.h>

namespace Hypertable {

  /// @addtogroup RangeServer
  /// @{

  /// Fills a block of scan results to be sent back to client.
  /// Iterates through <code>scanner</code> and serializes each key/value pair
  /// into <code>dbuf</code>, stopping when the buffer exceeds
  /// <code>buffer_size</code>.  If the KEYS_ONLY predicate is specified in the
  /// scan specification, then an empty value is encoded for each key/value
  /// pair.  For each key representing a COUNTER, the value is an encoded
  /// 64-bit integer and is converted to an ASCII value.
  /// @param scanner Scanner frome which results are to be obtained
  /// @param dbuf Buffer to hold encoded results
  /// @param cell_count Address of variable to hold number of cells in the scan
  /// block.
  /// @param buffer_size Target size of scan block
  /// @return <i>true</i> if there are more results to be pulled from the
  /// scanner when this function returns, <i>false</i> otherwise.
  bool FillScanBlock(CellListScannerPtr &scanner, DynamicBuffer &dbuf,
                     uint32_t *cell_count, int64_t buffer_size);

  /// @}

}

#endif // Hypertable_RangeServer_FillScanBlock_h

