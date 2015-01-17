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
/// Declarations for PhantomCommitRanges request parameters.
/// This file contains declarations for PhantomCommitRanges, a class for
/// encoding and decoding paramters to the <i>phantom commit ranges</i>
/// %RangeServer function.

#ifndef Hypertable_Lib_RangeServer_Request_Parameters_PhantomCommitRanges_h
#define Hypertable_Lib_RangeServer_Request_Parameters_PhantomCommitRanges_h

#include <Hypertable/Lib/QualifiedRangeSpec.h>
#include <Hypertable/Lib/RangeState.h>

#include <Common/Serializable.h>

#include <string>
#include <vector>

using namespace std;

namespace Hypertable {
namespace Lib {
namespace RangeServer {
namespace Request {
namespace Parameters {

  /// @addtogroup libHypertableRangeServerRequestParameters
  /// @{

  /// %Request parameters for <i>phantom commit ranges</i> function.
  class PhantomCommitRanges : public Serializable {
  public:

    /// Constructor.
    /// Empty initialization for decoding.
    PhantomCommitRanges() {}

    /// Constructor.
    /// Initializes with parameters for encoding.
    /// @param op_id %Operation ID
    /// @param location Location
    /// @param plan_generation Plan generation
    /// @param range_specs %Range specifications
    PhantomCommitRanges(int64_t op_id, const String &location,
                        int32_t plan_generation,
                        const vector<QualifiedRangeSpec> &range_specs)
      : m_op_id(op_id), m_location(location.c_str()),
        m_plan_generation(plan_generation), m_range_specs(range_specs) { }

    /// Gets operation ID
    /// @return %Operation ID
    int64_t op_id() const { return m_op_id; }

    /// Gets location
    /// @return Location
    const char *location() const { return m_location; }

    /// Gets plan generation
    /// @return Plan generation
    int32_t plan_generation() const { return m_plan_generation; }

    /// Gets range specifications
    /// @return Vector of range specifications
    const vector<QualifiedRangeSpec> &range_specs() const { return m_range_specs; }

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

    /// %Operation ID
    int64_t m_op_id;

    /// Location
    const char *m_location;

    /// Plan generation
    int32_t m_plan_generation;

    /// Vector of range specifications
    vector<QualifiedRangeSpec> m_range_specs;

  };

  /// @}

}}}}}

#endif // Hypertable_Lib_RangeServer_Request_Parameters_PhantomCommitRanges_h
