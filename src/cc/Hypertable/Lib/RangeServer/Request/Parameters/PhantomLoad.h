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
/// Declarations for PhantomLoad request parameters.
/// This file contains declarations for PhantomLoad, a class for encoding and
/// decoding paramters to the <i>phantom load</i> %RangeServer function.

#ifndef Hypertable_Lib_RangeServer_Request_Parameters_PhantomLoad_h
#define Hypertable_Lib_RangeServer_Request_Parameters_PhantomLoad_h

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

  /// %Request parameters for <i>phantom load</i> function.
  class PhantomLoad : public Serializable {
  public:

    /// Constructor.
    /// Empty initialization for decoding.
    PhantomLoad() {}

    /// Constructor.
    /// Initializes with parameters for encoding.
    /// @param location Location
    /// @param plan_generation Plan generation
    /// @param fragments Fragment IDs
    /// @param range_specs %Range specifications
    /// @param range_states %Range states
    PhantomLoad(const String &location, int32_t plan_generation,
                const vector<int32_t> &fragments,
                const vector<QualifiedRangeSpec> &range_specs,
                const vector<RangeState> &range_states)
      : m_location(location.c_str()), m_plan_generation(plan_generation),
        m_fragments(fragments), m_range_specs(range_specs),
        m_range_states(range_states) { }

    /// Gets location
    /// @return Location
    const char *location() const { return m_location; }

    /// Gets plan generation
    /// @return Plan generation
    int32_t plan_generation() const { return m_plan_generation; }

    /// Gets fragments
    /// @return Fragments
    const vector<int32_t> &fragments() const { return m_fragments; }

    /// Gets range specifications
    /// @return Vector of range specifications
    const vector<QualifiedRangeSpec> &range_specs() const { return m_range_specs; }

    /// Gets range states
    /// @return Vector of range states
    const vector<RangeState> &range_states() const { return m_range_states; }

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

    /// Location
    const char *m_location;

    /// Plan generation
    int32_t m_plan_generation;

    /// Fragments
    vector<int32_t> m_fragments;

    /// Vector of range specifications
    vector<QualifiedRangeSpec> m_range_specs;

    /// Vector of range states
    vector<RangeState> m_range_states;

  };

  /// @}

}}}}}

#endif // Hypertable_Lib_RangeServer_Request_Parameters_PhantomLoad_h
