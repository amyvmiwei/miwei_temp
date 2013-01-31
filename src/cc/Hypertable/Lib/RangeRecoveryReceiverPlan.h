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

#ifndef HYPERTABLE_RANGERECOVERYRECEIVERPLAN_H
#define HYPERTABLE_RANGERECOVERYRECEIVERPLAN_H

#include <vector>
#include <iostream>

#include <boost/multi_index_container.hpp>
#include <boost/multi_index/ordered_index.hpp>
#include <boost/multi_index/member.hpp>
#include <boost/multi_index/mem_fun.hpp>

#include "Common/StringExt.h"
#include "Common/PageArenaAllocator.h"

#include "Types.h"

namespace Hypertable {
  using namespace boost::multi_index;
  using namespace std;

  class RangeRecoveryReceiverPlan {
  public:
    RangeRecoveryReceiverPlan() { }

    void insert(const String &location, const TableIdentifier &table,
            const RangeSpec &range, const RangeState &state);

    void insert(const String &location, const QualifiedRangeSpec &qrs,
                const RangeState &state) {
      insert(location, qrs.table, qrs.range, state);
    }

    void remove(const QualifiedRangeSpec &qrs);

    void get_locations(StringSet &locations) const;

    bool get_location(const QualifiedRangeSpec &spec, String &location) const;

    void get_range_specs(vector<QualifiedRangeSpec> &specs);

    void get_range_specs(const String &location, vector<QualifiedRangeSpec> &specs);

    void get_range_specs_and_states(vector<QualifiedRangeSpec> &specs,
                                    vector<RangeState> &states);

    void get_range_specs_and_states(const String &location,
                                    vector<QualifiedRangeSpec> &specs,
                                    vector<RangeState> &states);

    bool get_range_spec(const TableIdentifier &table, const char *row,
                        QualifiedRangeSpec &spec);

    size_t encoded_length() const;
    void encode(uint8_t **bufp) const;
    void decode(const uint8_t **bufp, size_t *remainp);

    void clear() { m_plan.clear(); }
    size_t size() const { return m_plan.size(); }
    bool empty() const { return m_plan.empty(); }
    void copy(RangeRecoveryReceiverPlan &other) const;

  private:
    class ReceiverEntry {
    public:

      ReceiverEntry(CharArena &arena, const String &location_,
          const TableIdentifier &table_, const RangeSpec &range_,
          const RangeState &state_) {
        location = location_;
        spec.table.id = arena.dup(table_.id);
        spec.table.generation = table_.generation;
        spec.range.start_row = arena.dup(range_.start_row);
        spec.range.end_row = arena.dup(range_.end_row);
        state.state = state_.state;
        state.timestamp = state_.timestamp;
        state.soft_limit = state_.soft_limit;
        state.transfer_log = arena.dup(state_.transfer_log);
        state.split_point = arena.dup(state_.split_point);
        state.old_boundary_row = arena.dup(state_.old_boundary_row);
      }

      ReceiverEntry() { }

      size_t encoded_length() const;
      void encode(uint8_t **bufp) const;
      void decode(const uint8_t **bufp, size_t *remainp);

      String location;
      QualifiedRangeSpec spec;
      RangeState state;
      friend ostream &operator<< (ostream &os, const ReceiverEntry &entry) {
        os << "{ReceiverEntry:" << entry.spec << entry.state << ", location="
            << entry.location << "}";
        return os;
      }
    };

    struct ByRange {};
    struct ByLocation {};
    typedef multi_index_container<
      ReceiverEntry,
      indexed_by<
        ordered_unique<tag<ByRange>, member<ReceiverEntry,
                  QualifiedRangeSpec, &ReceiverEntry::spec> >,
        ordered_non_unique<tag<ByLocation>,
                  member<ReceiverEntry, String, &ReceiverEntry::location> >
      >
    > ReceiverPlan;
    typedef ReceiverPlan::index<ByRange>::type RangeIndex;
    typedef ReceiverPlan::index<ByLocation>::type LocationIndex;

    ReceiverPlan m_plan;
    CharArena m_arena;

  public:
    friend ostream &operator<<(ostream &os, const RangeRecoveryReceiverPlan &plan) {
      RangeRecoveryReceiverPlan::LocationIndex &location_index =
          (const_cast<RangeRecoveryReceiverPlan&>(plan)).m_plan.get<1>();
      RangeRecoveryReceiverPlan::LocationIndex::const_iterator location_it =
        location_index.begin();
      os << "{RangeRecoveryReceiverPlan: num_entries="
          << location_index.size();
      while (location_it != location_index.end()) {
        os << *location_it;
        ++location_it;
      }
      os << "}";
      return os;
    }
  };


} // namespace Hypertable


#endif // HYPERTABLE_RANGERECOVERYRECEIVERPLAN_H
