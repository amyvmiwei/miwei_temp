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
/// Declarations for ReplayStatus request parameters.
/// This file contains declarations for ReplayStatus, a class for encoding and
/// decoding paramters to the <i>replay status</i> %Master operation.

#ifndef Hypertable_Lib_Master_Request_Parameters_ReplayStatus_h
#define Hypertable_Lib_Master_Request_Parameters_ReplayStatus_h

#include <Common/Serializable.h>

#include <string>

using namespace std;

namespace Hypertable {
namespace Lib {
namespace Master {
namespace Request {
namespace Parameters {

  /// @addtogroup libHypertableMasterRequestParameters
  /// @{

  /// %Request parameters for <i>replay status</i> operation.
  class ReplayStatus : public Serializable {
  public:

    /// Constructor.
    /// Empty initialization for decoding.
    ReplayStatus() {}

    /// Constructor.
    /// Initializes with parameters for encoding.  Sets #m_op_id to
    /// <code>op_id</code>, #m_location to <code>location</code>, and
    /// #m_plan_generation to <code>plan_generation</code>.
    /// @param op_id Recovery operation ID
    /// @param location Proxy name of %RangeServer whose log is being replayed
    /// @param plan_generation Recovery plan generation
    ReplayStatus(int64_t op_id, const std::string &location, int32_t plan_generation)
      : m_op_id(op_id), m_location(location), m_plan_generation(plan_generation)
    { }

    /// Gets recovery operation ID
    /// @return Recovery operation ID
    int64_t op_id() { return m_op_id; }

    /// Gets proxy name of %RangeServer whose log is being replayed
    /// @return Proxy name of %RangeServer whose log is being replayed
    const string& location() const { return m_location; }

    /// Gets recovery plan generation
    /// @return Recovery plan generation
    int32_t plan_generation() { return m_plan_generation; }

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

    /// Recovery operation ID
    int64_t m_op_id {};

    /// Proxy name of %RangeServer whose log is being replayed
    string m_location;

    /// Recovery plan generation
    int32_t m_plan_generation {};
  };

  /// @}

}}}}}

#endif // Hypertable_Lib_Master_Request_Parameters_ReplayStatus_h
