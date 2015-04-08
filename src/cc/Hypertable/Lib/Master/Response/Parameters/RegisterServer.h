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
/// Declarations for RegisterServer response parameters.
/// This file contains declarations for RegisterServer, a class for encoding and
/// decoding paramters returned by the <i>register server</i> %Master operation.

#ifndef Hypertable_Lib_Master_Response_Parameters_RegisterServer_h
#define Hypertable_Lib_Master_Response_Parameters_RegisterServer_h

#include <Hypertable/Lib/SystemVariable.h>

#include <Common/Serializable.h>
#include <Common/StatsSystem.h>

#include <string>
#include <vector>

using namespace std;

namespace Hypertable {
namespace Lib {
namespace Master {
namespace Response {
namespace Parameters {

  /// @addtogroup libHypertableMasterResponseParameters
  /// @{

  /// %Response parameters for <i>register server</i> operation.
  class RegisterServer : public Serializable {
  public:

    /// Constructor.
    /// Empty initialization for decoding.
    RegisterServer() {}

    /// Constructor.
    /// Initializes with parameters for encoding.
    /// @param location Location (proxy name)
    /// @param generation %System state generation
    /// @param variables %System state variable specifications
    RegisterServer(const string &location, int64_t generation,
                   vector<SystemVariable::Spec> &variables)
      : m_location(location), m_generation(generation),
        m_variables(variables) { }

    /// Gets location (proxy name)
    /// @return Location (proxy name)
    const string& location() const { return m_location; }

    /// Gets system state generation
    /// @return %System state generation
    int64_t generation() { return m_generation; }

    /// Gets state variables
    /// @return State variables
    const vector<SystemVariable::Spec> &variables() { return m_variables; }

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

    /// Sytem state generation
    int64_t m_generation {};

    /// %System state variable specifications
    vector<SystemVariable::Spec> m_variables;

  };

  /// @}

}}}}}

#endif // Hypertable_Lib_Master_Response_Parameters_RegisterServer_h
