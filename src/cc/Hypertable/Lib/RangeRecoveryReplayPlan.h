/** -*- c++ -*-
 * Copyright (C) 2007-2012 Hypertable, Inc.
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

#ifndef HYPERTABLE_RANGESERVERRECOVERYREPLAYPLAN_H
#define HYPERTABLE_RANGESERVERRECOVERYREPLAYPLAN_H

#include <vector>
#include <iostream>
#include <boost/multi_index_container.hpp>
#include <boost/multi_index/ordered_index.hpp>
#include <boost/multi_index/member.hpp>

#include "Common/StringExt.h"

namespace Hypertable {
  using namespace boost::multi_index;
  using namespace std;

  /**
   * RangeRecoveryReplayPlan is a class that is used to hold a
   * recovery "replay" plan.  This plan consists of a set of fragments
   * and associated range servers that are responsible for replaying
   * the fragment as part of the recovery operation.
   */
  class RangeRecoveryReplayPlan {
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
    void insert(uint32_t fragment, const String &location);

    /**
     *
     * @param location proxy name of range server to remove
     */
    void remove_location(const String &location);

    /**
     * Fills a vector with all of the fragment numbers that are part of this
     * replay plan
     *
     * @param fragments reference to vector to hold fragment numbers
     */
    void get_fragments(vector<uint32_t> &fragments) const;

    /**
     * Fills a vector with fragment numbers assigned to a specific location
     * in this replay plan
     *
     * @param location location (range server) for which to get fragment numbers
     * @param fragments reference to vector to hold fragment numbers
     */
    void get_fragments(const String &location, vector<uint32_t> &fragments) const;

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
    bool get_location(uint32_t fragment, String &location) const;

    size_t encoded_length() const;
    void encode(uint8_t **bufp) const;
    void decode(const uint8_t **bufp, size_t *remainp);
    void clear() { m_plan.clear(); }

  private:
    class ReplayEntry {
      public:
        ReplayEntry(const String &location_, uint32_t fragment_=0) : location(location_),
        fragment(fragment_) { }
        ReplayEntry(uint32_t fragment_) : fragment(fragment_) { }
        ReplayEntry() : fragment(0) { }

        size_t encoded_length() const;
        void encode(uint8_t **bufp) const;
        void decode(const uint8_t **bufp, size_t *remainp);

        String location;
        uint32_t fragment;
        friend ostream &operator<<(ostream &os, const ReplayEntry &entry) {
          os << "{ReplayEntry:location=" << entry.location<< ", fragment="
             << entry.fragment<< "}";
          return os;
        }
    };

    struct ByFragment {};
    struct ByLocation {};
    typedef boost::multi_index_container<
      ReplayEntry,
      indexed_by<
        ordered_unique<tag<ByFragment>,
                       member<ReplayEntry, uint32_t, &ReplayEntry::fragment> >,
        ordered_non_unique<tag<ByLocation>,
                           member<ReplayEntry, String, &ReplayEntry::location> >
      >
    > ReplayPlan;
    typedef ReplayPlan::index<ByFragment>::type FragmentIndex;
    typedef ReplayPlan::index<ByLocation>::type LocationIndex;

    ReplayPlan m_plan;

  public:
    friend ostream &operator<<(ostream &os, const RangeRecoveryReplayPlan &plan) {
      RangeRecoveryReplayPlan::LocationIndex &index =
          (const_cast<RangeRecoveryReplayPlan&>(plan)).m_plan.get<1>();
      RangeRecoveryReplayPlan::LocationIndex::const_iterator iter = index.begin();

      os << "{RangeRecoveryReplayPlan: num_entries=" << index.size();
      while (iter!=index.end()) {
        os << *iter;
        ++iter;
      }
      os << "}";
      return os;
    }
  };


} // namespace Hypertable


#endif // HYPERTABLE_RANGESERVERRECOVERYREPLAYPLAN_H
