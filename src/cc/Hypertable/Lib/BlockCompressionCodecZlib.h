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
/// Declarations for BlockCompressionCodecZlib.
/// This file contains declarations for BlockCompressionCodecZlib, a class for
/// compressing blocks using the ZLIB compression algorithm.

#ifndef HYPERTABLE_BLOCKCOMPRESSIONCODECZLIB_H
#define HYPERTABLE_BLOCKCOMPRESSIONCODECZLIB_H

#include <Hypertable/Lib/BlockCompressionCodec.h>

extern "C" {
#include <zlib.h>
}

namespace Hypertable {

  /// @addtogroup libHypertable
  /// @{

  /// Block compressor that uses the ZLIB algorithm.
  /// This class provides a way to compress and decompress blocks of data using
  /// the libz compression algorithm, a general purpose, dictionary-based
  /// compression algorithm that provides a good compression ratio at the
  /// expense of speed.
  class BlockCompressionCodecZlib : public BlockCompressionCodec {

  public:
    BlockCompressionCodecZlib(const Args &args);
    virtual ~BlockCompressionCodecZlib();

    /// Sets arguments to control compression behavior.
    /// The arguments accepted by this method are described in the following
    /// table.
    /// <table>
    /// <tr>
    /// <th>Argument</th><th>Description</th>
    /// </tr>
    /// <tr>
    /// <td><code>--best</code> or <code>--9</code> </td><td>Best compression
    /// ratio</td>
    /// </tr>
    /// <tr>
    /// <td><code>--normal</code> </td><td>Normal compression ratio</td>
    /// </tr>
    /// </table>
    /// @param args Vector of arguments
    virtual void set_args(const Args &args);

    /// Compresses a buffer using the ZLIB algorithm.
    /// This method first checks #m_deflate_initialized.  If is <i>false</i> it
    /// initializes #m_stream_deflate and sets #m_deflate_initialized to
    /// <i>true</i>.  It then reserves enough space in <code>output</code> to
    /// hold the serialized <code>header</code> followed by the compressed input
    /// followed by <code>reserve</code> bytes.  If the resulting compressed
    /// buffer is larger than the input buffer, then the input buffer is copied
    /// directly to the output buffer and the compression type is set to
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

    /// Decompresses a buffer compressed with the ZLIB algorithm.
    /// This method first checks #m_inflate_initialized.  If is <i>false</i> it
    /// initializes #m_stream_inflate and sets #m_inflate_initialized to
    /// <i>true</i>.  It then decompresses the <code>input</code> buffer and
    /// fills in <code>header</code>.
    /// @see deflate() for description of input buffer format
    /// @param intput Input buffer
    /// @param output Output buffer
    /// @param header Block header
    virtual void inflate(const DynamicBuffer &input, DynamicBuffer &output,
                         BlockHeader &header);

    /// Returns enum value representing compression type ZLIB.
    /// Returns the enum value ZLIB
    /// @see BlockCompressionCodec::ZLIB
    /// @return Compression type (ZLIB)
    virtual int get_type() { return ZLIB; }

  private:

    /// Inflate state
    z_stream m_stream_inflate;

    /// Flag indicating that inflate state has been initialized
    bool m_inflate_initialized;

    /// Deflate state
    z_stream m_stream_deflate;

    /// Flag indicating that deflate state has been initialized
    bool m_deflate_initialized;

    /// Compression level
    int m_level;
  };

  /// @}

}

#endif // HYPERTABLE_BLOCKCOMPRESSIONCODECZLIB_H

