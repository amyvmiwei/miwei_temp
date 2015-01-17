/* -*- c++ -*-
 * Copyright (C) 2007-2014 Hypertable, Inc.
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
/// Declarations for Seek request parameters.
/// This file contains declarations for Seek, a class for encoding and
/// decoding paramters to the <i>seek</i> file system broker function.

#ifndef FsBroker_Lib_Request_Parameters_Seek_h
#define FsBroker_Lib_Request_Parameters_Seek_h

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

  /// %Request parameters for <i>seek</i> requests.
  class Seek : public Serializable {
  public:

    /// Constructor.
    /// Empty initialization for decoding.
    Seek() {}

    /// Constructor.
    /// Initializes with parameters for encoding.  Sets #m_fd to
    /// <code>fd</code> and #m_offset to <code>offset</code>.
    /// @param fd File descriptor
    /// @param offset File offset
    Seek(int32_t fd, uint64_t offset) : m_fd(fd), m_offset(offset) {}

    /// Gets file descriptor
    /// @return File descriptor
    int32_t get_fd() { return m_fd; }

    /// Gets file offset
    /// @return File offset
    uint64_t get_offset() { return m_offset; }

  private:

    uint8_t encoding_version() const override;

    size_t encoded_length_internal() const override;

    void encode_internal(uint8_t **bufp) const override;

    void decode_internal(uint8_t version, const uint8_t **bufp,
			 size_t *remainp) override;

    /// File descriptor to which seek applies
    int32_t m_fd {};

    /// File offset
    uint64_t m_offset;

  };

  /// @}

}}}}}

#endif // FsBroker_Lib_Request_Parameters_Seek_h
