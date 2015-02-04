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
/// Declarations for Shutdown request parameters.
/// This file contains declarations for Shutdown, a class for encoding and
/// decoding paramters to the <i>shutdown</i> file system broker function.

#ifndef FsBroker_Lib_Request_Parameters_Shutdown_h
#define FsBroker_Lib_Request_Parameters_Shutdown_h

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

  /// %Request parameters for <i>shutdown</i> requests.
  class Shutdown : public Serializable {
  public:

    /// Constructor.
    /// Empty initialization for decoding.
    Shutdown() {}

    /// Constructor.
    /// Initializes with parameters for encoding.  Sets #m_flags to
    /// <code>flags</code>.
    /// @param flags Flags
    Shutdown(uint16_t flags) : m_flags(flags) {}

    /// Gets flags
    /// @return Flags
    uint16_t get_flags() { return m_flags; }

  private:

    uint8_t encoding_version() const override;

    size_t encoded_length_internal() const override;

    void encode_internal(uint8_t **bufp) const override;

    void decode_internal(uint8_t version, const uint8_t **bufp,
			 size_t *remainp) override;

    /// Flags
    uint16_t m_flags {};

  };

  /// @}

}}}}}

#endif // FsBroker_Lib_Request_Parameters_Shutdown_h
