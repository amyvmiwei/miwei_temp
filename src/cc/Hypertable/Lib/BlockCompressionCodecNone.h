/* -*- c++ -*-
 * Copyright (C) 2007-2013 Hypertable, Inc.
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
/// Declarations for BlockCompressionCodecNone.
/// This file contains declarations for BlockCompressionCodecNone, a class for
/// compressing blocks using the NONE compression algorithm.

#ifndef HYPERTABLE_BLOCKCOMPRESSIONCODECNONE_H
#define HYPERTABLE_BLOCKCOMPRESSIONCODECNONE_H

#include <Hypertable/Lib/BlockCompressionCodec.h>

namespace Hypertable {

  /// @addtogroup libHypertable
  /// @{

  /// Block compressor that performs no compression.
  /// This class exists provides a way to produce uncompressed serialized
  /// blocks using the BlockCompressionCodec framework.
  class BlockCompressionCodecNone : public BlockCompressionCodec {

  public:

    /// Constructor.
    /// This constructor passes <code>args</code> to the default implementation
    /// of set_args() since it does not support any arguments.
    /// @param args Arguments to control compression behavior
    /// @throws Exception Code set to Error::BLOCK_COMPRESSOR_INVALID_ARG
    BlockCompressionCodecNone(const Args &args)  { set_args(args); }

    /// Transforms a buffer into a serialized block using no compression.
    /// This method reserves enough space in <code>output</code> to hold the
    /// serialized <code>header</code> followed by the input buffer followed
    /// by <code>reserve</code> bytes.  The input buffer is copied directly
    /// to the output buffer and the compression type is set to
    /// BlockCompressionCodec::NONE.  Before serailizing <code>header</code>,
    /// the <i>data_length</i>, <i>data_zlength</i>, <i>data_checksum</i>, and
    /// <i>compression_type</i> fields are set appropriately.  The output buffer
    /// is formatted as follows:
    /// <table>
    /// <tr>
    /// <td>header</td><td>uncompressed data</td><td>reserve</td>
    /// </tr>
    /// </table>
    /// @param intput Input buffer
    /// @param output Output buffer
    /// @param header Block header populated by function
    /// @param reserve Additional space to reserve at end of <code>output</code>
    ///   buffer
    virtual void deflate(const DynamicBuffer &input, DynamicBuffer &output,
                         BlockHeader &header, size_t reserve=0);

    /// Deserializes a block produced by deflate().
    /// @see deflate() for description of input buffer %format
    /// @param intput Input buffer
    /// @param output Output buffer
    /// @param header Block header
    virtual void inflate(const DynamicBuffer &input, DynamicBuffer &output,
                         BlockHeader &header);

    /// Returns enum value representing compression type NONE.
    /// Returns the enum value NONE
    /// @see BlockCompressionCodec::NONE
    /// @return Compression type (NONE)
    virtual int get_type() { return NONE; }
  };

  /// @}

}

#endif // HYPERTABLE_BLOCKCOMPRESSIONCODECNONE_H

