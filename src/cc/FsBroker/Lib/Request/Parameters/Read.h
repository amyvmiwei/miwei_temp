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
/// Declarations for Read request parameters.
/// This file contains declarations for Read, a class for encoding and
/// decoding paramters to the <i>read</i> file system broker function.

#ifndef FsBroker_Lib_Request_Parameters_Read_h
#define FsBroker_Lib_Request_Parameters_Read_h

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

  /// %Request parameters for <i>read</i> requests.
  class Read : public Serializable {
  public:

    /// Constructor.
    /// Empty initialization for decoding.
    Read() {}

    /// Constructor.
    /// Initializes with parameters for encoding.  Sets #m_fd to
    /// <code>fd</code> and #m_amount to <code>amount</code>.
    /// @param fd File descriptor
    /// @param amount Amount of data to read
    Read(int32_t fd, uint32_t amount) : m_fd(fd), m_amount(amount) {}

    /// Gets file descriptor
    /// @return File descriptor
    int32_t get_fd() { return m_fd; }

    /// Gets amount of data to read
    /// @return Amount of data to read
    uint32_t get_amount() { return m_amount; }

  private:

    uint8_t encoding_version() const override;

    size_t encoded_length_internal() const override;

    void encode_internal(uint8_t **bufp) const override;

    void decode_internal(uint8_t version, const uint8_t **bufp,
			 size_t *remainp) override;

    /// File descriptor to which read applies
    int32_t m_fd {};

    /// Amount of data to read
    uint32_t m_amount;

  };

  /// @}

}}}}}

#endif // FsBroker_Lib_Request_Parameters_Read_h
