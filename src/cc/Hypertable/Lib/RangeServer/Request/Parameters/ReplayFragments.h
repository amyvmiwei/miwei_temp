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
/// Declarations for ReplayFragments request parameters.
/// This file contains declarations for ReplayFragments, a class for encoding and
/// decoding paramters to the <i>replay fragments</i> %RangeServer function.

#ifndef Hypertable_Lib_RangeServer_Request_Parameters_ReplayFragments_h
#define Hypertable_Lib_RangeServer_Request_Parameters_ReplayFragments_h

#include <Hypertable/Lib/RangeServerRecovery/ReceiverPlan.h>

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

  /// %Request parameters for <i>replay fragments</i> function.
  class ReplayFragments : public Serializable {
  public:

    /// Constructor.
    /// Empty initialization for decoding.
    ReplayFragments() {}

    /// Constructor.
    /// Initializes with parameters for encoding.
    /// @param op_id %Operation ID
    /// @param location Location
    /// @param plan_generation Plan generation
    /// @param type Range type
    /// @param fragments Fragments
    /// @param receiver_plan Receiver plan
    /// @param replay_timeout Replay timeout
    ReplayFragments(int64_t op_id, const string &location,
                    int32_t plan_generation, int32_t type,
                    const vector<int32_t> &fragments,
                    const Lib::RangeServerRecovery::ReceiverPlan &receiver_plan,
                    int32_t replay_timeout)
      : m_op_id(op_id), m_location(location.c_str()),
        m_plan_generation(plan_generation), m_type(type),
        m_fragments(fragments), m_receiver_plan(receiver_plan),
        m_replay_timeout(replay_timeout) { }

    /// Gets operation ID
    /// @return %Operation ID
    int64_t op_id() { return m_op_id; }

    /// Gets location
    /// @return Location
    const char *location() { return m_location; }

    /// Gets plan generation
    /// @return Plan generation
    int32_t plan_generation() { return m_plan_generation; }

    /// Gets range type
    /// @return Range type
    int32_t type() { return m_type; }

    /// Gets fragments
    /// @return Fragments
    const vector<int32_t> &fragments() { return m_fragments; }

    /// Gets receiver plan
    /// @return Receiver plan
    const Lib::RangeServerRecovery::ReceiverPlan &receiver_plan() { return m_receiver_plan; }

    /// Gets replay timeout
    /// @return Replay timeout
    int32_t replay_timeout() { return m_replay_timeout; }

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

    /// Range type
    int32_t m_type;

    /// Fragments
    vector<int32_t> m_fragments;

    /// Receiver plan
    Lib::RangeServerRecovery::ReceiverPlan m_receiver_plan;

    /// Replay timeout
    int32_t m_replay_timeout;

  };

  /// @}

}}}}}

#endif // Hypertable_Lib_RangeServer_Request_Parameters_ReplayFragments_h
