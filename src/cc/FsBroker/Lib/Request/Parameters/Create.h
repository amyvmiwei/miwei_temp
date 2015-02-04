/* -*- c++ -*-
 * Copyright (C) 2007-2015 Hypertable, Inc.
 *
 * This file is part of Hypertable.
 *
 * Hypertable is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 3
 * of the License, or any later version.
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
/// Declarations for Create request parameters.
/// This file contains declarations for Create, a class for encoding and
/// decoding paramters to the <i>create</i> file system broker function.

#ifndef FsBroker_Lib_Request_Parameters_Create_h
#define FsBroker_Lib_Request_Parameters_Create_h

#include <Common/Serializable.h>

#include <string>

using namespace std;

namespace Hypertable {
namespace FsBroker {
namespace Lib {
namespace Request {
namespace Parameters {

  /// @addtogroup FsBrokerLibRequestParameters
  /// @{

  /// %Request parameters for <i>create</i> requests.
  class Create : public Serializable {
  public:

    /// Constructor.
    /// Empty initialization for decoding.
    Create() {}

    /// Constructor.
    /// Initializes with parameters for encoding.  Sets #m_fname to
    /// <code>fname</code>, #m_flags to <code>flags</code>, #m_bufsz to
    /// <code>bufsz</code>, #m_replication to <code>replication</code>, and
    /// #m_blksz to <code>blksz</code>.
    /// @param fname File name
    /// @param flags Create flags
    /// @param bufsz Buffer size
    /// @param replication Replication factor
    /// @param blksz Block size
    Create(const String &fname, uint32_t flags, int32_t bufsz,
	   int32_t replication, int64_t blksz)
      : m_fname(fname), m_flags(flags), m_bufsz(bufsz),
	m_replication(replication), m_blksz(blksz) {}

    /// Gets file name
    /// @return File name
    const char *get_name() { return m_fname.c_str(); }

    /// Gets create flags
    /// @return Create flags
    uint32_t get_flags() { return m_flags; }

    /// Gets buffer size
    /// @return Buffer size
    int32_t get_buffer_size() { return m_bufsz; }

    /// Gets replication factor
    /// @return Replication factor
    int32_t get_replication() { return m_replication; }

    /// Gets block size
    /// @return Block size
    int64_t get_block_size() { return m_blksz; }

  private:

    uint8_t encoding_version() const override;

    size_t encoded_length_internal() const override;

    void encode_internal(uint8_t **bufp) const override;

    void decode_internal(uint8_t version, const uint8_t **bufp,
			 size_t *remainp) override;

    /// File name
    String m_fname;

    /// Create flags
    uint32_t m_flags;

    /// Buffer size
    int32_t m_bufsz;

    /// Replication
    int32_t m_replication;

    /// Block size
    int64_t m_blksz;
  };

  /// @}

}}}}}

#endif // FsBroker_Lib_Request_Parameters_Create_h
