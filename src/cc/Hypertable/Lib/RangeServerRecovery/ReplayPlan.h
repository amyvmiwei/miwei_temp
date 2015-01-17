/* -*- c++ -*-
 * Copyright (C) 2007-2015 Hypertable, Inc.
 *
 * This file is part of Hypertable.
 *
 * Hypertable is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; version 2 of the
 * License, or any later version.
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
/// Declarations for ReplayPlan.
/// This file contains declarations for ReplayPlan, a class that
/// holds information about how the commit log fragments from a failed
/// %RangeServer will be replayed during recovery.

#ifndef Hypertable_Lib_RangeServerRecovery_ReplayPlan_h
#define Hypertable_Lib_RangeServerRecovery_ReplayPlan_h

#include "FragmentReplayPlan.h"

#include <Common/Serializable.h>
#include <Common/StringExt.h>

#include <vector>
#include <iostream>

namespace Hypertable {
namespace Lib {
namespace RangeServerRecovery {

  using namespace boost::multi_index;
  using namespace std;

  /// @addtogroup libHypertableRangeServerRecovery
  /// @{

  /// Holds a fragment replay plan for the recovery of a %RangeServer.
  /// This plan consists of a set of fragments and associated range servers that
  /// are responsible for replaying the fragment as part of the recovery
  /// operation.
  class ReplayPlan : public Serializable {
  public:

    /**
     * Modifies the recovery replay plan by inserting a fragment and associated
     * location (range server) responsible for replaying that fragment.
     * If the fragment already exists in the plan, then the associated
     * location information is updated with the new location.
     *
     * @param fragment fragment to replay
     * @param location proxy name of range server responsble for replaying
     *        fragment
     */
    void insert(int32_t fragment, const string &location);

    /**
     *
     * @param location proxy name of range server to remove
     */
    void remove_location(const string &location);

    /**
     * Fills a vector with all of the fragment numbers that are part of this
     * replay plan
     *
     * @param fragments reference to vector to hold fragment numbers
     */
    void get_fragments(vector<int32_t> &fragments) const;

    /**
     * Fills a vector with fragment numbers assigned to a specific location
     * in this replay plan
     *
     * @param location location (range server) for which to get fragment numbers
     * @param fragments reference to vector to hold fragment numbers
     */
    void get_fragments(const string &location, vector<int32_t> &fragments) const;

    /**
     * Fills a set of location strings that represent all of the locations (range
     * servers) that are part of this plan.
     *
     * @param locations reference to string set to be filled with locations
     */
    void get_locations(StringSet &locations) const;

    /**
     * Given a fragment number, return location it is assigned to
     *
     * @param fragment fragment number to look up
     * @param location reference to string to hold returned location
     * @return <i>true</i> if found, <i>false</i> otherwise
     */
    bool get_location(int32_t fragment, string &location) const;

    /// Clears the plan.
    void clear() { container.clear(); }

    /// Container holding fragment replay plans
    FragmentReplayPlanContainer container;

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

  public:
    friend ostream &operator<<(ostream &os, const ReplayPlan &plan) {
      auto & location_index =
          (const_cast<ReplayPlan&>(plan)).container.get<1>();
      os << "{ReplayPlan: num_entries=" << location_index.size();
      for (auto & entry : location_index)
        os << entry;
      os << "}";
      return os;
    }
  };

  /// @}

}}}

#endif // Hypertable_Lib_RangeServerRecovery_ReplayPlan_h
