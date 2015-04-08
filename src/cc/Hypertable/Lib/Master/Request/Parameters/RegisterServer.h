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
/// Declarations for RegisterServer request parameters.
/// This file contains declarations for RegisterServer, a class for encoding and
/// decoding paramters to the <i>register server</i> %Master operation.

#ifndef Hypertable_Lib_Master_Request_Parameters_RegisterServer_h
#define Hypertable_Lib_Master_Request_Parameters_RegisterServer_h

#include <Common/Serializable.h>
#include <Common/StatsSystem.h>

#include <string>

using namespace std;

namespace Hypertable {
namespace Lib {
namespace Master {
namespace Request {
namespace Parameters {

  /// @addtogroup libHypertableMasterRequestParameters
  /// @{

  /// %Request parameters for <i>register server</i> operation.
  class RegisterServer : public Serializable {
  public:

    /// Constructor.
    /// Empty initialization for decoding.
    RegisterServer() {}

    /// Constructor.
    /// Initializes with parameters for encoding.
    /// @param location Location (proxy name)
    /// @param listen_port Listen port
    /// @param lock_held Lock held flag
    /// @param system_stats %System stats
    RegisterServer(const std::string &location, uint16_t listen_port, bool lock_held,
                   StatsSystem &system_stats)
      : m_location(location), m_listen_port(listen_port),
        m_lock_held(lock_held), m_system_stats(system_stats) { }

    /// Gets location (proxy name)
    /// @return Location (proxy name)
    const string& location() const { return m_location; }

    /// Gets listen port
    /// @return Listen port
    uint16_t listen_port() { return m_listen_port; }

    /// Gets lock held flag
    /// @return Lock held flag
    bool lock_held() { return m_lock_held; }

    /// Gets system stats
    /// @return %System stats
    const StatsSystem &system_stats() const { return m_system_stats; }

    /// Gets server current time
    /// @return Server current time
    int64_t now() { return m_now; }

  private:

    /// Returns encoding version.
    /// @return Encoding version
    uint8_t encoding_version() const override;

    /// Returns internal serialized length.
    /// @return Internal serialized length
    /// @see encode_internal() for encoding format
    size_t encoded_length_internal() const override;

    /// Writes serialized representation of object to a buffer.
    /// @param bufp Address of destination buffer pointer (advanced by call)
    void encode_internal(uint8_t **bufp) const override;

    /// Reads serialized representation of object from a buffer.
    /// @param version Encoding version
    /// @param bufp Address of destination buffer pointer (advanced by call)
    /// @param remainp Address of integer holding amount of serialized object
    /// remaining
    /// @see encode_internal() for encoding format
    void decode_internal(uint8_t version, const uint8_t **bufp,
			 size_t *remainp) override;

    /// Location (proxy name)
    string m_location;

    /// Listen port
    uint16_t m_listen_port;

    /// Lock held flag
    bool m_lock_held;

    /// %System stats
    StatsSystem m_system_stats;

    /// Current time of registering server
    int64_t m_now;

  };

  /// @}

}}}}}

#endif // Hypertable_Lib_Master_Request_Parameters_RegisterServer_h
