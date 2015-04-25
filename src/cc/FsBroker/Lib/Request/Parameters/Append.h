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
/// Declarations for Append request parameters.
/// This file contains declarations for Append, a class for encoding and
/// decoding paramters to the <i>append</i> file system broker function.

#ifndef FsBroker_Lib_Request_Parameters_Append_h
#define FsBroker_Lib_Request_Parameters_Append_h

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

  /// %Request parameters for <i>append</i> requests.
  class Append : public Serializable {
  public:

    /// Constructor.
    /// Empty initialization for decoding.
    Append() {}

    /// Constructor.
    /// Initializes with parameters for encoding.  Sets #m_fd to
    /// <code>fd</code>, #m_size to <code>size</code>, and #m_flush to
    /// <code>flush</code>.
    /// @param fd File descriptor
    /// @param size Size of data buffer
    /// @param flags Flags (FLUSH or SYNC)
    Append(int32_t fd, uint32_t size, uint8_t flags)
      : m_fd(fd), m_size(size), m_flags(flags) {}

    /// Gets file descriptor
    /// @return File descriptor
    int32_t get_fd() { return m_fd; }

    /// Gets size of data buffer
    /// @return Size of data buffer
    uint32_t get_size() { return m_size; }

    /// Gets flags
    /// @return Flags
    uint8_t get_flags() { return m_flags; }

  private:

    uint8_t encoding_version() const override;

    size_t encoded_length_internal() const override;

    void encode_internal(uint8_t **bufp) const override;

    void decode_internal(uint8_t version, const uint8_t **bufp,
			 size_t *remainp) override;

    /// File descriptor to which append applies
    int32_t m_fd {};

    /// Size of data buffer
    uint32_t m_size {};

    /// Flags (FLUSH or SYNC)
    uint8_t m_flags {};
  };

  /// @}

}}}}}

#endif // FsBroker_Lib_Request_Parameters_Append_h
