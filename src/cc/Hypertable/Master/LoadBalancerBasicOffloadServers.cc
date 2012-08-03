/** -*- c++ -*-
 * Copyright (C) 2007-2012 Hypertable, Inc.
 *
 * This file is part of Hypertable.
 *
 * Hypertable is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; version 3 of the
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

#include <algorithm>

#include "Hypertable/Lib/Client.h"
#include "LoadBalancerBasicOffloadServers.h"

using namespace Hypertable;
using namespace std;

void LoadBalancerBasicOffloadServers::compute_plan(vector<RangeServerStatistics> &range_server_stats,
        set<String> &offload_servers, const String &root_location,
        BalancePlanPtr &balance_plan) {

  size_t num_servers = range_server_stats.size();
  if (num_servers <= 1)
    return;
  // Now scan thru METADATA Location and StartRow and come up with balance plan
  compute_moves(range_server_stats, offload_servers, root_location, balance_plan);
}

void LoadBalancerBasicOffloadServers::compute_moves(vector<RangeServerStatistics> &range_server_stats,
        set<String> &offload_servers, const String &root_location,
        BalancePlanPtr &plan) {

  HT_INFOF("Offloading ranges from %d servers", (int)offload_servers.size());

  set<String> load_servers;
  set<String>::iterator load_servers_it;

  foreach_ht(const RangeServerStatistics &stats, range_server_stats) {
    if (offload_servers.find(stats.location) == offload_servers.end()) {
      if (m_context->can_accept_ranges(stats))
        load_servers.insert(stats.location);
    }
  }

  // return if there's no server with enough disk space
  if (load_servers.empty())
    return;

  load_servers_it = load_servers.begin();

  ScanSpec scan_spec;
  Cell cell;
  String last_key, last_location, last_start_row;
  bool read_start_row=false;
  String location("Location"), start_row("StartRow");

  // check if we need to move root range
  if (offload_servers.find(root_location) != offload_servers.end()) {
    String new_location = *load_servers_it;
    load_servers_it++;
    if (load_servers_it == load_servers.end())
      load_servers_it = load_servers.begin();

    RangeMoveSpecPtr move = new RangeMoveSpec(root_location.c_str(),
            new_location.c_str(), TableIdentifier::METADATA_ID, "",
            Key::END_ROOT_ROW);
    plan->moves.push_back(move);

    HT_DEBUG_OUT << TableIdentifier::METADATA_ID << "[" << ".."
        << Key::END_ROOT_ROW << "] " << root_location << "->"
        << new_location << HT_END;
  }

  scan_spec.columns.push_back(location.c_str());
  scan_spec.columns.push_back(start_row.c_str());
  scan_spec.max_versions = 1;

  TableScannerPtr scanner = m_context->metadata_table->create_scanner(scan_spec);
  while (scanner->next(cell)) {

    if (last_key == cell.row_key) {
      if (location == cell.column_family)
        last_location = String((const char *)cell.value, cell.value_len);
      else {
        read_start_row = true;
        last_start_row = String((const char *)cell.value, cell.value_len);
      }
    }
    else {
      last_key = cell.row_key;
      if (location == cell.column_family) {
        last_start_row.clear();
        read_start_row = false;
        last_location = String((const char *)cell.value, cell.value_len);
      }
      else {
        last_location.clear();
        read_start_row = true;
        last_start_row = String((const char *)cell.value, cell.value_len);
      }
    }

    HT_DEBUG_OUT << "last_key=" << last_key << ", last_location="
        << last_location << ", last_start_row=" << last_start_row << HT_END;

    // check if this range is on one of the offload servers
    if (last_location.size() > 0 && read_start_row &&
        offload_servers.find(last_location) != offload_servers.end()) {
      size_t pos = last_key.find(':');
      HT_ASSERT(pos != string::npos);
      String table(last_key, 0, pos);
      String end_row(last_key, pos+1);
      String new_location = *load_servers_it;
      load_servers_it++;
      if (load_servers_it == load_servers.end())
        load_servers_it = load_servers.begin();

      RangeMoveSpecPtr move = new RangeMoveSpec(last_location.c_str(),
              new_location.c_str(), table.c_str(), last_start_row.c_str(),
              end_row.c_str());
      plan->moves.push_back(move);

      HT_DEBUG_OUT << table << "[" << last_start_row << ".." << end_row << "] "
          << last_location << "->" << new_location << HT_END;
    }
  }
}
