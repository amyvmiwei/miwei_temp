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
#include "LoadBalancerBasicDistributeTableRanges.h"

using namespace Hypertable;
using namespace std;

void LoadBalancerBasicDistributeTableRanges::compute_plan(vector<RangeServerStatistics> &range_server_stats,
        BalancePlanPtr &balance_plan) {
  HT_INFO("Distributing table ranges");

  clear();

  m_num_servers = range_server_stats.size();
  if (m_num_servers <= 1)
    return;

  compute_range_distribution(range_server_stats);

  m_num_servers = m_servers.size();
  HT_INFO_OUT << "m_num_servers=" << m_num_servers << ", m_num_empty_servers="
      << m_num_empty_servers << ", total ranges=" << m_num_ranges << HT_END;
  if (m_servers.size() <= 1)
    return;

  // Now scan thru METADATA Location and StartRow and come up with balance plan
  compute_moves(balance_plan);
}

void LoadBalancerBasicDistributeTableRanges::compute_moves(BalancePlanPtr &plan) {
  ScanSpec scan_spec;
  Cell cell;
  String last_key, last_location, last_start_row;
  bool read_start_row = false;
  String location("Location"), start_row("StartRow");

  scan_spec.columns.push_back(location.c_str());
  scan_spec.columns.push_back(start_row.c_str());
  scan_spec.max_versions = 1;

  TableScannerPtr scanner = m_context->metadata_table->create_scanner(scan_spec);
  while (scanner->next(cell)) {

    // don't move root METADATA range
    if (!strcmp(cell.row_key, Key::END_ROOT_ROW)) {
      HT_DEBUG_OUT << "Skipping METADATA root range " << cell.row_key << HT_END;
      continue;
    }

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

    if (last_location.size() > 0 && read_start_row) {
      size_t pos = last_key.find(':');
      HT_ASSERT(pos != string::npos);
      String table(last_key, 0, pos);
      String end_row(last_key, pos+1);
      bool added;
      added = maybe_add_to_plan(table.c_str(), last_location.c_str(),
              last_start_row.c_str(), end_row.c_str(), plan);
      if (added)
        HT_DEBUG_OUT << "Added move for range " << table << ":"
            << end_row << ", start_row=" << last_start_row << ", location="
            << last_location << HT_END;
    }
  }
}

bool LoadBalancerBasicDistributeTableRanges::maybe_add_to_plan(const char *table,
        const char *src_server, const char *start_row, const char *end_row,
        BalancePlanPtr &plan) {
  // no one can take any more ranges from this table
  if (m_saturated_tables.find(table) != m_saturated_tables.end()) {
    HT_DEBUG_OUT << "Moves for table " << table << " saturated"
         << " Dont move " << src_server << ":"<< table << "[" << start_row
         << ".." << end_row << "] " << HT_END;
    return false;
  }

  // we dont have summary info on this table
  TableSummaryMap::iterator table_it = m_table_summaries.find(table);
  if (table_it == m_table_summaries.end()) {
    HT_DEBUG_OUT << "Summary info for table " << table << " not found."
         << " Dont move " << src_server << ":"<< table << "[" << start_row
         << ".." << end_row << "] " << HT_END;
    return false;
  }
  // src server isn't overloaded on ranges from this table
  if (table_it->second->ranges_per_server == 0)
    table_it->second->ranges_per_server = table_it->second->total_ranges / m_num_servers + 1;
  uint32_t ranges_per_server = table_it->second->ranges_per_server;
  RangeDistributionMap::iterator src_range_dist_it = table_it->second->range_dist.find(src_server);
  if (src_range_dist_it == table_it->second->range_dist.end()) {
    HT_DEBUG_OUT << "No monitoring info for range."
         << " Dont move " << src_server << ":"<< table << "[" << start_row
         << ".." << end_row << "] " << HT_END;
    return false;
  }
  if (src_range_dist_it->second <= ranges_per_server) {
    HT_DEBUG_OUT << "src server has " << src_range_dist_it->second
         << " ranges from this table which is below average "
         << ranges_per_server << " Dont move " << src_server << ":" << table
         << "[" << start_row << ".." << end_row << "] " << HT_END;
    return false;
  }

  // find a destination server that can take this range
  RangeDistributionMap::iterator dst_range_dist_it;
  String dst_server;

  bool found_dst_server = false;
  foreach_ht(const char *target_server, m_servers) {
    if (!strcmp(target_server, src_server))
      continue;
    dst_range_dist_it = table_it->second->range_dist.find(target_server);
    if (dst_range_dist_it == table_it->second->range_dist.end()) {
      pair<RangeDistributionMap::iterator, bool> ret =
          table_it->second->range_dist.insert(make_pair(target_server, 0));
      dst_range_dist_it = ret.first;
    }
    if (dst_range_dist_it->second < ranges_per_server) {
      found_dst_server = true;
      dst_server = target_server;
      break;
    }
  }

  // none of the empty servers can accomodate any more ranges from this table
  if (!found_dst_server) {
    m_saturated_tables.insert(table_it->first);
    HT_DEBUG_OUT << "Can't find destination for ranges from table " << table
        << " (saturated)." << " Dont move " << src_server << ":" << table
        << "[" << start_row << ".." << end_row << "] " << HT_END;
    return false;
  }

  // update range distribution info and add move to balance plan
  --(src_range_dist_it->second);
  ++(dst_range_dist_it->second);
  RangeMoveSpecPtr move = new RangeMoveSpec(src_server, dst_server.c_str(),
          table, start_row, end_row);
  plan->moves.push_back(move);

  // randomly shuffle the contents of the empty_servers vector to avoid
  // adjacent ranges from accumulating on the same empty range server
  random_shuffle(m_servers.begin(), m_servers.end());

  return true;
}

void LoadBalancerBasicDistributeTableRanges::clear() {
  m_servers.clear();
  m_saturated_tables.clear();
  m_table_summaries.clear();
  m_num_servers = m_num_empty_servers = 0;
  m_num_ranges = 0;
}

void LoadBalancerBasicDistributeTableRanges::compute_range_distribution(vector<RangeServerStatistics> &range_server_stats) {

  m_num_ranges = 0;
  foreach_ht (RangeServerStatistics &rs, range_server_stats) {
    if (!rs.stats || !rs.stats->live || !m_context->can_accept_ranges(rs))
      continue;
    const char *server_id = rs.location.c_str();
    m_servers.push_back(server_id);
    if (rs.stats->range_count > 0) {
      m_num_ranges += rs.stats->range_count;
      foreach_ht (StatsTable &table, rs.stats->tables) {
        uint32_t range_count = table.range_count;
        const char *table_id = table.table_id.c_str();
        HT_DEBUG_OUT << "Looking at stats for " << server_id << ":"
            << table_id << HT_END;
        TableSummaryMap::iterator it = m_table_summaries.find(table_id);
        if (it == m_table_summaries.end()) {
          TableSummaryPtr ts = new TableSummary;
          pair<TableSummaryMap::iterator, bool>  ret =
            m_table_summaries.insert(make_pair(table_id, ts));
          it = ret.first;
          HT_DEBUG_OUT << "Created TableSummary for " << table_id << HT_END;
        }
        it->second->range_dist.insert(make_pair(server_id, range_count));
        it->second->total_ranges += range_count;
      }
    }
    else {
      ++m_num_empty_servers;
      HT_DEBUG_OUT << "Found empty server " << server_id << HT_END;
    }
  }
  HT_DEBUG_OUT << " m_table_summaries.size=" << m_table_summaries.size()
      << HT_END;
}
