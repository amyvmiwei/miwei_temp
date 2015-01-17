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

/// @file
/// Definitions for ReplayPlan.
/// This file contains definitions for ReplayPlan, a class that
/// holds information about how the commit log fragments from a failed
/// %RangeServer will be replayed during recovery.

#include <Common/Compat.h>

#include "ReplayPlan.h"

#include <Common/Serialization.h>

using namespace std;

namespace Hypertable {
namespace Lib {
namespace RangeServerRecovery {

void ReplayPlan::insert(int32_t fragment, const String &location) {
  FragmentReplayPlan entry(location, fragment);
  auto &fragment_index = container.get<FragmentReplayPlanById>();
  auto fragment_it = fragment_index.find(entry.fragment);
  if (fragment_it != fragment_index.end())
    container.erase(fragment_it);
  container.insert(entry);
}

void ReplayPlan::remove_location(const String &location) {
  auto &location_index = container.get<FragmentReplayPlanByLocation>();
  pair<FragmentReplayPlanLocationIndex::const_iterator, FragmentReplayPlanLocationIndex::const_iterator> bounds =
      location_index.equal_range(location);
  auto iter = bounds.first;
  while (iter != bounds.second)
    iter = location_index.erase(iter);
}

void ReplayPlan::get_fragments(vector<int32_t> &fragments) const {
  auto &fragment_index = container.get<FragmentReplayPlanById>();
  for (auto & entry : fragment_index)
    fragments.push_back(entry.fragment);
}

void ReplayPlan::get_fragments(const String &location,
                               vector<int32_t> &fragments) const {
  auto &location_index = container.get<FragmentReplayPlanByLocation>();
  pair<FragmentReplayPlanLocationIndex::const_iterator, FragmentReplayPlanLocationIndex::const_iterator> bounds =
      location_index.equal_range(location);
  for (auto iter = bounds.first; iter != bounds.second; ++iter)
    fragments.push_back(iter->fragment);
}

void ReplayPlan::get_locations(StringSet &locations) const {
  auto &location_index = container.get<FragmentReplayPlanByLocation>();
  String last_location;
  for (auto & entry : location_index) {
    if (entry.location != last_location) {
      last_location = entry.location;
      locations.insert(last_location);
    }
  }
}

bool ReplayPlan::get_location(int32_t fragment, String &location) const {
  auto &fragment_index = container.get<FragmentReplayPlanById>();
  auto fragment_it = fragment_index.find(fragment);
  bool found = false;
  if (fragment_it != fragment_index.end()) {
    location = fragment_it->location;
    found = true;
  }
  return found;
}

uint8_t ReplayPlan::encoding_version() const {
  return 1;
}

size_t ReplayPlan::encoded_length_internal() const {
  size_t length = 4;
  auto &location_index = container.get<FragmentReplayPlanByLocation>();
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
/// <td>Entry count</td>
/// </tr>
/// <tr>
/// <td>For each entry:</td>
/// </tr>
/// <tr>
/// <td>FragmentReplayPlan</td>
/// <td>Fragment replay information</td>
/// </tr>
/// </table>
void ReplayPlan::encode_internal(uint8_t **bufp) const {
  auto &location_index = container.get<FragmentReplayPlanByLocation>();
  Serialization::encode_i32(bufp, location_index.size());
  for (auto & entry : location_index)
    entry.encode(bufp);
}

void ReplayPlan::decode_internal(uint8_t version, const uint8_t **bufp,
                                 size_t *remainp) {
  size_t count = Serialization::decode_i32(bufp, remainp);
  for (size_t i=0; i<count; ++i) {
    FragmentReplayPlan entry;
    entry.decode(bufp, remainp);
    container.insert(entry);
  }
}

}}}
