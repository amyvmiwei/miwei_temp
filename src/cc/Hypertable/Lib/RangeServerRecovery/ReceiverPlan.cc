/*
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

#include "Common/Compat.h"
#include "ReceiverPlan.h"

using namespace Hypertable;
using namespace Hypertable::Lib::RangeServerRecovery;
using namespace std;


ReceiverPlan::ReceiverPlan(const ReceiverPlan &other) {
  auto &location_index = other.container.get<ServerReceiverPlanByLocation>();
  for (auto & entry : location_index) {
    ServerReceiverPlan copied(arena, entry.location, entry.spec.table,
                              entry.spec.range, entry.state);
    container.insert(copied);
  }
}

void ReceiverPlan::insert(const String &location, const TableIdentifier &table,
                          const RangeSpec &range, const RangeState &state) {
  ServerReceiverPlan entry(arena, location, table, range, state);
  auto &range_index = container.get<ServerReceiverPlanByRange>();
  auto iter = range_index.find(entry.spec);
  if (iter != range_index.end())
    range_index.erase(iter);
  container.insert(entry);
}


void ReceiverPlan::remove(const QualifiedRangeSpec &qrs) {
  auto &range_index = container.get<ServerReceiverPlanByRange>();
  auto iter = range_index.find(qrs);
  if (iter != range_index.end())
    range_index.erase(iter);
}


void ReceiverPlan::get_locations(StringSet &locations) const {
  auto &location_index = container.get<ServerReceiverPlanByLocation>();
  String last_location;
  for (auto & entry : location_index) {
    if (entry.location != last_location) {
      last_location = entry.location;
      locations.insert(last_location);
    }
  }
}

bool ReceiverPlan::get_location(const QualifiedRangeSpec &spec, String &location) const {
  auto &range_index = container.get<ServerReceiverPlanByRange>();
  auto range_it = range_index.find(spec);
  if (range_it != range_index.end()) {
    location = range_it->location;
    return true;
  }
  return false;
}


void ReceiverPlan::get_range_specs(vector<QualifiedRangeSpec> &specs) const {
  auto &index = container.get<ServerReceiverPlanByLocation>();
  for (auto & entry : index)
    specs.push_back(entry.spec);
}

void ReceiverPlan::get_range_specs(const String &location, vector<QualifiedRangeSpec> &specs) const {
  auto &location_index = container.get<ServerReceiverPlanByLocation>();
  pair<ServerReceiverPlanLocationIndex::iterator, ServerReceiverPlanLocationIndex::iterator> bounds =
    location_index.equal_range(location);
  for (auto iter = bounds.first; iter != bounds.second; ++iter)
    specs.push_back(iter->spec);
}

void ReceiverPlan::get_range_specs_and_states(vector<QualifiedRangeSpec> &specs,
                                              vector<RangeState> &states) const {
  auto &index = container.get<ServerReceiverPlanByLocation>();
  for (auto & entry : index) {
    specs.push_back(entry.spec);
    states.push_back(entry.state);
  }
}


void ReceiverPlan::get_range_specs_and_states(const String &location,
                                              vector<QualifiedRangeSpec> &specs,
                                              vector<RangeState> &states) const {
  auto &location_index = container.get<ServerReceiverPlanByLocation>();
  pair<ServerReceiverPlanLocationIndex::iterator, ServerReceiverPlanLocationIndex::iterator> bounds =
    location_index.equal_range(location);
  for (auto iter = bounds.first; iter != bounds.second; ++iter) {
    specs.push_back(iter->spec);
    states.push_back(iter->state);
  }
}

bool ReceiverPlan::get_range_spec(const TableIdentifier &table, const char *row,
                                  QualifiedRangeSpec &spec) const {
  auto &range_index = container.get<ServerReceiverPlanByRange>();
  QualifiedRangeSpec target(table, RangeSpec("",row));
  auto range_it = range_index.lower_bound(target);

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

void ReceiverPlan::copy(ReceiverPlan &other) const {
  auto &location_index = container.get<ServerReceiverPlanByLocation>();
  for (auto & entry : location_index) {
    ServerReceiverPlan copied(other.arena, entry.location, entry.spec.table,
                              entry.spec.range, entry.state);
    other.container.insert(copied);
  }
}

uint8_t ReceiverPlan::encoding_version() const {
  return 1;
}

size_t ReceiverPlan::encoded_length_internal() const {
  auto &location_index = container.get<ServerReceiverPlanByLocation>();
  size_t length = 4;
  for (auto & entry : location_index)
    length += entry.encoded_length();
  return length;
}

/// @details
/// Encoding is as follows:
/// <table>
/// <tr>
/// <th>Encoding</th>
/// <th>Description</th>
/// </tr>
/// <tr>
/// <td>i32</td>
/// <td>Server receiver plan count</td>
/// </tr>
/// <tr>
/// <td>For each server receiver plan</td>
/// </tr>
/// <tr>
/// <td>ServerReceiverPlan</td>
/// <td>Server receiver plan</td>
/// </tr>
/// </table>
void ReceiverPlan::encode_internal(uint8_t **bufp) const {
  auto &location_index = container.get<ServerReceiverPlanByLocation>();
  Serialization::encode_i32(bufp, location_index.size());
  for (auto & entry : location_index)
    entry.encode(bufp);
}

void ReceiverPlan::decode_internal(uint8_t version, const uint8_t **bufp,
                                      size_t *remainp) {
  size_t count = Serialization::decode_i32(bufp, remainp);
  for (size_t i = 0; i<count; ++i) {
    ServerReceiverPlan tmp;
    tmp.decode(bufp, remainp);
    ServerReceiverPlan entry(arena, tmp.location, tmp.spec.table,
                             tmp.spec.range, tmp.state);
    container.insert(entry);
  }
}
