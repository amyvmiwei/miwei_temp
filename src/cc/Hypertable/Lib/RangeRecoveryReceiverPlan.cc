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

#include "Common/Compat.h"
#include "RangeRecoveryReceiverPlan.h"

using namespace Hypertable;
using namespace boost::multi_index;
using namespace std;

void RangeRecoveryReceiverPlan::insert(const String &location,
    const TableIdentifier &table, const RangeSpec &range, const RangeState &state) {
  ReceiverEntry entry(m_arena, location, table, range, state);
  RangeIndex &range_index = m_plan.get<ByRange>();
  RangeIndex::iterator range_it = range_index.find(entry.spec);
  if (range_it != range_index.end())
    range_index.erase(range_it);
  m_plan.insert(entry);
}


void RangeRecoveryReceiverPlan::remove(const QualifiedRangeSpec &qrs) {
  RangeIndex &range_index = m_plan.get<ByRange>();
  RangeIndex::iterator range_it = range_index.find(qrs);
  if (range_it != range_index.end())
    range_index.erase(range_it);
}


void RangeRecoveryReceiverPlan::get_locations(StringSet &locations) const {
  const LocationIndex &location_index = m_plan.get<ByLocation>();
  LocationIndex::const_iterator location_it = location_index.begin();
  String last_location;
  for(; location_it != location_index.end(); ++location_it) {
    if (location_it->location != last_location) {
      last_location = location_it->location;
      locations.insert(last_location);
    }
  }
}

bool RangeRecoveryReceiverPlan::get_location(const QualifiedRangeSpec &spec, String &location) const {
  const RangeIndex &range_index = m_plan.get<ByRange>();
  RangeIndex::iterator range_it = range_index.find(spec);
  if (range_it != range_index.end()) {
    location = range_it->location;
    return true;
  }
  return false;
}


void RangeRecoveryReceiverPlan::get_range_specs(vector<QualifiedRangeSpec> &specs) {
  LocationIndex &index = m_plan.get<ByLocation>();
  for (LocationIndex::iterator iter = index.begin(); iter != index.end(); ++iter)
    specs.push_back(iter->spec);
}

void RangeRecoveryReceiverPlan::get_range_specs(const String &location, vector<QualifiedRangeSpec> &specs) {
  LocationIndex &location_index = m_plan.get<ByLocation>();
  pair<LocationIndex::iterator, LocationIndex::iterator> bounds =
    location_index.equal_range(location);
  LocationIndex::iterator location_it = bounds.first;
  for(; location_it != bounds.second; ++location_it)
    specs.push_back(location_it->spec);
}

void RangeRecoveryReceiverPlan::get_range_specs_and_states(vector<QualifiedRangeSpec> &specs,
                                                           vector<RangeState> &states) {
  LocationIndex &index = m_plan.get<ByLocation>();
  for (LocationIndex::iterator iter = index.begin(); iter != index.end(); ++iter) {
    specs.push_back(iter->spec);
    states.push_back(iter->state);
  }
}


void RangeRecoveryReceiverPlan::get_range_specs_and_states(const String &location,
                                                           vector<QualifiedRangeSpec> &specs,
                                                           vector<RangeState> &states) {
  LocationIndex &location_index = m_plan.get<ByLocation>();
  pair<LocationIndex::iterator, LocationIndex::iterator> bounds =
    location_index.equal_range(location);
  LocationIndex::iterator location_it = bounds.first;
  for(; location_it != bounds.second; ++location_it) {
    specs.push_back(location_it->spec);
    states.push_back(location_it->state);
  }
}

bool RangeRecoveryReceiverPlan::get_range_spec(const TableIdentifier &table, const char *row,
                                               QualifiedRangeSpec &spec) {
  RangeIndex &range_index = m_plan.get<ByRange>();
  QualifiedRangeSpec target(table, RangeSpec("",row));
  RangeIndex::iterator range_it = range_index.lower_bound(target);

  bool found = false;

  while(range_it != range_index.end() && range_it->spec.table == table) {
    if (strcmp(row, range_it->spec.range.start_row) > 0 &&
        strcmp(row, range_it->spec.range.end_row) <= 0 ) {
      spec = range_it->spec;
      found = true;
      break;
    }
    else if (strcmp(row, range_it->spec.range.start_row)<=0) {
      // gone too far
      break;
    }
    ++range_it;
  }
  /**
  if (!found) {
    range_it = range_index.begin();
    HT_DEBUG_OUT << " Range not found " << table.id << " " << row
        << " existing ranges are..." << HT_END;
    while(range_it != range_index.end()) {
      HT_DEBUG_OUT << range_it->spec << HT_END;
      ++range_it;
    }
  }
  **/

  return found;
}


size_t RangeRecoveryReceiverPlan::encoded_length() const {
  const LocationIndex &location_index = m_plan.get<ByLocation>();
  LocationIndex::const_iterator location_it = location_index.begin();
  size_t len = 4;

  while(location_it != location_index.end()) {
    len += location_it->encoded_length();
    ++location_it;
  }
  return len;
}

void RangeRecoveryReceiverPlan::encode(uint8_t **bufp) const {
  const LocationIndex &location_index = m_plan.get<ByLocation>();
  LocationIndex::const_iterator location_it = location_index.begin();

  Serialization::encode_i32(bufp, location_index.size());

  while (location_it != location_index.end()) {
    location_it->encode(bufp);
    ++location_it;
  }
}

void RangeRecoveryReceiverPlan::decode(const uint8_t **bufp, size_t *remainp) {
  size_t num_entries = Serialization::decode_i32(bufp, remainp);
  for (size_t ii = 0; ii < num_entries; ++ii) {
    ReceiverEntry tmp_entry;
    tmp_entry.decode(bufp, remainp);
    ReceiverEntry entry(m_arena, tmp_entry.location,
                        tmp_entry.spec.table, tmp_entry.spec.range,
                        tmp_entry.state);
    m_plan.insert(entry);
  }
}

void RangeRecoveryReceiverPlan::copy(RangeRecoveryReceiverPlan &other) const
{
  const LocationIndex &location_index = m_plan.get<ByLocation>();
  LocationIndex::const_iterator it = location_index.begin();

  while (it != location_index.end()) {
    ReceiverEntry entry(other.m_arena, it->location, it->spec.table,
                        it->spec.range, it->state);
    other.m_plan.insert(entry);
    ++it;
  }
}

size_t RangeRecoveryReceiverPlan::ReceiverEntry::encoded_length() const {
  return Serialization::encoded_length_vstr(location) + spec.encoded_length() +
    state.encoded_length();
}

void RangeRecoveryReceiverPlan::ReceiverEntry::encode(uint8_t **bufp) const {
  Serialization::encode_vstr(bufp,location);
  spec.encode(bufp);
  state.encode(bufp);
}

void RangeRecoveryReceiverPlan::ReceiverEntry::decode(const uint8_t **bufp,
    size_t *remainp) {
  location = Serialization::decode_vstr(bufp, remainp);
  spec.decode(bufp, remainp);
  state.decode(bufp, remainp);
}
