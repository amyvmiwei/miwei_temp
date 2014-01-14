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
/// Declarations for BlockCompressionCodecBmz.
/// This file contains declarations for BlockCompressionCodecBmz, a class for
/// compressing blocks using the BMZ compression algorithm.

#ifndef HYPERTABLE_BLOCKCOMPRESSIONCODECBMZ_H
#define HYPERTABLE_BLOCKCOMPRESSIONCODECBMZ_H

#include <Hypertable/Lib/BlockCompressionCodec.h>

#include <Common/DynamicBuffer.h>

namespace Hypertable {

  /// @addtogroup libHypertable
  /// @{

  /// Block compressor that uses the BMZ algorithm.
  /// This class provides a way to compress and decompress blocks of data using
  /// the <i>bmz</i> algorithm, a compression algorithm based on the one
  /// described in the paper, <i>Data Compression Using Long Common Strings</i>
  /// (Bentley & McIlroy, 1999).  This algorithm generally works well for data
  /// that contains long repeated strings.  It was described in, <i>Bigtable: A
  /// Distributed Storage %System for Structured Data</i> (Dean et al., 2006) as
  /// the compression algorithm they use for the "content" column of their
  /// crawler database.  In this column they store multiple copies of each
  /// crawled page content.
  class BlockCompressionCodecBmz : public BlockCompressionCodec {
  public:

    /// Constructor.
    /// Initializes members as follows:  #m_workmem (0), #m_offset=0,
    /// #m_fp_len=19.  It then calls bmz_init() and then passes
    /// <code>args</code> into a call to set_args().
    /// @param args Arguments to control compression behavior
    /// @see set_args
    BlockCompressionCodecBmz(const Args &args);

    /// Destructor.
    virtual ~BlockCompressionCodecBmz();

    /// Compresses a buffer using the BMZ algorithm.
    /// This method reserves enough space in <code>output</code> to hold the
    /// serialized <code>header</code> followed by the compressed input followed
    /// by <code>reserve</code> bytes.  If the resulting compressed buffer is
    /// larger than the input buffer, then the input buffer is copied directly
    /// to the output buffer and the compression type is set to
    /// BlockCompressionCodec::NONE.  Before serailizing <code>header</code>,
    /// the <i>data_length</i>, <i>data_zlength</i>, <i>data_checksum</i>, and
    /// <i>compression_type</i> fields are set appropriately.  The output buffer
    /// is formatted as follows:
    /// <table>
    /// <tr>
    /// <td>header</td><td>compressed data</td><td>reserve</td>
    /// </tr>
    /// </table>
    /// @param intput Input buffer
    /// @param output Output buffer
    /// @param header Block header populated by function
    /// @param reserve Additional space to reserve at end of <code>output</code>
    ///   buffer
    virtual void deflate(const DynamicBuffer &input, DynamicBuffer &output,
                         BlockHeader &header, size_t reserve=0);

    /// Decompresses a buffer compressed with the BMZ algorithm.
    /// @see deflate() for description of input buffer %format
    /// @param intput Input buffer
    /// @param output Output buffer
    /// @param header Block header
    virtual void inflate(const DynamicBuffer &input, DynamicBuffer &output,
                         BlockHeader &header);

    /// Sets arguments to control compression behavior.
    /// The arguments accepted by this method are described in the following
    /// table.
    /// <table>
    /// <tr>
    /// <th>Argument</th><th>Description</th>
    /// </tr>
    /// <tr>
    /// <td><code>--fp-len</code> <i>n</i></td><td>Fingerprint length</td>
    /// </tr>
    /// <tr>
    /// <td><code>--offset</code> <i>n</i></td><td>Starting offset of
    ///   fingerprints</td>
    /// </tr>
    /// </table>
    /// @param args Vector of arguments
    virtual void set_args(const Args &args);

    /// Returns enum value representing compression type BMZ.
    /// Returns the enum value BMZ
    /// @see BlockCompressionCodec::BMZ
    /// @return Compression type (BMZ)
    virtual int get_type() { return BMZ; }

  private:

    /// Working memory buffer used by deflate() and inflate()
    DynamicBuffer m_workmem;

    /// Starting offset of fingerprints
    size_t m_offset;

    /// Fingerprint length
    size_t m_fp_len;
  };

  /// @}

} // namespace Hypertable

#endif // HYPERTABLE_BLOCKCOMPRESSIONCODECBMZ_H
