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
/// Declarations for SetState request parameters.
/// This file contains declarations for SetState, a class for encoding and
/// decoding paramters to the <i>set state</i> %Master operation.

#ifndef Hypertable_Lib_Master_Request_Parameters_SetState_h
#define Hypertable_Lib_Master_Request_Parameters_SetState_h

#include <Hypertable/Lib/SystemVariable.h>

#include <Common/Serializable.h>

#include <string>
#include <vector>

using namespace std;

namespace Hypertable {
namespace Lib {
namespace Master {
namespace Request {
namespace Parameters {

  /// @addtogroup libHypertableMasterRequestParameters
  /// @{

  /// %Request parameters for <i>set state</i> operation.
  class SetState : public Serializable {
  public:

    /// Constructor.
    /// Empty initialization for decoding.
    SetState() {}

    /// Constructor.
    /// Initializes with parameters for encoding.  Sets #m_specs to
    /// <code>specs</code>.
    /// @param specs %System variable specifications
    SetState(const std::vector<SystemVariable::Spec> &specs) : m_specs(specs) { }

    /// Gets system variable specifications
    /// @return %System variable specifications
    const std::vector<SystemVariable::Spec> &specs() { return m_specs; }

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

    /// %System variable specifications
    std::vector<SystemVariable::Spec> m_specs;

  };

  /// @}

}}}}}

#endif // Hypertable_Lib_Master_Request_Parameters_SetState_h
