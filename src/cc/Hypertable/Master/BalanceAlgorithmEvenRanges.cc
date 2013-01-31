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
#include <vector>

#include "Hypertable/Lib/Client.h"
#include "BalanceAlgorithmEvenRanges.h"

using namespace Hypertable;
using namespace std;


namespace {

  typedef std::map<String, uint32_t> RangeDistributionMap;

  class TableSummary : public ReferenceCount {
  public:
    TableSummary() : total_ranges(0), ranges_per_server(0) { }
    uint32_t total_ranges;
    uint32_t ranges_per_server;
    RangeDistributionMap range_dist;
  };

  typedef intrusive_ptr<TableSummary> TableSummaryPtr;

  // map of table_id to TableSummary
  typedef std::map<const String, TableSummaryPtr> TableSummaryMap;

  typedef struct {
    vector<String> servers;
    TableSummaryMap table_summaries;
    size_t num_ranges;
    uint32_t num_empty_servers;
  } balance_state_t;

  bool maybe_add_to_plan(const String &table, const String &src_server,
                         const String &start_row, const String &end_row,
                         balance_state_t &state, BalancePlanPtr &plan) {
    StringSet saturated_tables;

    // no one can take any more ranges from this table
    if (saturated_tables.find(table) != saturated_tables.end()) {
      HT_DEBUG_OUT << "Moves for table " << table << " saturated"
                   << " Dont move " << src_server << ":"<< table << "[" << start_row
                   << ".." << end_row << "] " << HT_END;
      return false;
    }

    // we dont have summary info on this table
    TableSummaryMap::iterator table_it = state.table_summaries.find(table);
    if (table_it == state.table_summaries.end()) {
      HT_DEBUG_OUT << "Summary info for table " << table << " not found."
                   << " Dont move " << src_server << ":"<< table << "[" << start_row
                   << ".." << end_row << "] " << HT_END;
      return false;
    }

    // src server isn't overloaded on ranges from this table
    if (table_it->second->ranges_per_server == 0)
      table_it->second->ranges_per_server = table_it->second->total_ranges / state.servers.size() + 1;

    uint32_t ranges_per_server =  table_it->second->ranges_per_server;
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
    foreach_ht(const String &target_server, state.servers) {
      if (!target_server.compare(src_server))
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
      saturated_tables.insert(table_it->first);
      HT_DEBUG_OUT << "Can't find destination for ranges from table " << table
                   << " (saturated)." << " Dont move " << src_server << ":" << table
                   << "[" << start_row << ".." << end_row << "] " << HT_END;
      return false;
    }

    // update range distribution info and add move to balance plan
    --(src_range_dist_it->second);
    ++(dst_range_dist_it->second);
    RangeMoveSpecPtr move = new RangeMoveSpec(src_server.c_str(), dst_server.c_str(),
                                              table.c_str(), start_row.c_str(), end_row.c_str());
    plan->moves.push_back(move);

    // randomly shuffle the contents of the empty_servers vector to avoid
    // adjacent ranges from accumulating on the same empty range server
    random_shuffle(state.servers.begin(), state.servers.end());

    return true;
  }

}


BalanceAlgorithmEvenRanges::BalanceAlgorithmEvenRanges(ContextPtr &context,
                                std::vector<RangeServerStatistics> &statistics)
  : m_context(context), m_statistics(statistics) {
}



void BalanceAlgorithmEvenRanges::compute_plan(BalancePlanPtr &plan,
     std::vector<RangeServerConnectionPtr> &balanced) {
  balance_state_t state;
  StringSet locations;
  int32_t min_ranges = -1;
  int32_t max_ranges = -1;

  state.num_ranges = 0;
  state.num_empty_servers = 0;

  foreach_ht (RangeServerStatistics &rs, m_statistics) {

    if (!rs.stats->live) {
      HT_INFOF("Aborting balance because %s is not yet live", rs.location.c_str());
      plan->moves.clear();
      return;
    }

    if (!rs.stats || !m_context->can_accept_ranges(rs))
      continue;

    locations.insert(rs.location);

    if (min_ranges == -1)
      min_ranges = max_ranges = rs.stats->range_count;
    else {
      if (min_ranges > rs.stats->range_count)
        min_ranges = rs.stats->range_count;
      if (max_ranges < rs.stats->range_count)
        max_ranges = rs.stats->range_count;
    }

    if (rs.stats->range_count > 0) {
      TableSummaryPtr ts;
      state.num_ranges += rs.stats->range_count;
      foreach_ht (StatsTable &table, rs.stats->tables) {
        TableSummaryMap::iterator it = state.table_summaries.find(table.table_id.c_str());
        if (it == state.table_summaries.end()) {
          ts = new TableSummary;
          state.table_summaries[table.table_id.c_str()] = ts;
        }
        else
          ts = it->second;
        ts->range_dist.insert(make_pair(rs.location, table.range_count));
        ts->total_ranges += table.range_count;
      }
    }
    else
      state.num_empty_servers++;
  }

  if (locations.size() <= 1)
    HT_THROWF(Error::MASTER_BALANCE_PREVENTED,
              "Not enough available servers (%u)", (unsigned)locations.size());

  /**
   * Populate the 'balanced' vector with the participants that
   * are not balanced
   */
  std::vector<RangeServerConnectionPtr> connections;
  m_context->rsc_manager->get_valid_connections(locations, connections);
  foreach_ht (RangeServerConnectionPtr &rsc, connections) {
    if (!rsc->get_balanced())
      balanced.push_back(rsc);
    state.servers.push_back(rsc->location());
  }

  // Only balance if the max variance is at least 3
  if ((max_ranges - min_ranges) < 3) {
    HT_INFO("Aborting balance because max variance < 3");
    return;
  }
  
  /**
   * Now scan thru METADATA Location and StartRow and come up with balance plan
   */
  {
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
        if (maybe_add_to_plan(table, last_location, last_start_row, end_row, state, plan)) {
          HT_DEBUG_OUT << "Added move for range " << table << ":" << end_row
                       << ", start_row=" << last_start_row << ", location="
                       << last_location << HT_END;
        }
      }
    }
  }
}
