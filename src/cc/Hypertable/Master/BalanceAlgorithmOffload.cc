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
#include "Common/Error.h"

#include <boost/algorithm/string.hpp>

#include <algorithm>

#include "Hypertable/Lib/Client.h"

#include "BalanceAlgorithmOffload.h"
#include "Utility.h"

using namespace Hypertable;
using namespace std;

BalanceAlgorithmOffload::BalanceAlgorithmOffload(ContextPtr &context,
 std::vector<RangeServerStatistics> &statistics, String arguments)
  : m_context(context), m_statistics(statistics) {
  boost::trim(arguments);
  boost::split(m_offload_servers, arguments, boost::is_any_of(", \t"));
}


void BalanceAlgorithmOffload::compute_plan(BalancePlanPtr &plan,
                            std::vector<RangeServerConnectionPtr> &balanced) {
  StringSet locations;
  StringSet::iterator locations_it;
  String root_location = Utility::root_range_location(m_context);

  foreach_ht(const RangeServerStatistics &stats, m_statistics) {
    if (m_offload_servers.find(stats.location) == m_offload_servers.end()) {
      if (!stats.stats || !stats.stats->live || !m_context->can_accept_ranges(stats))
        continue;
      locations.insert(stats.location);
    }
  }

  if (locations.empty())
    HT_THROW(Error::MASTER_BALANCE_PREVENTED,
             "No destination servers found with enough room");

  locations_it = locations.begin();

  ScanSpec scan_spec;
  Cell cell;
  String last_key, last_location, last_start_row;
  bool read_start_row = false;
  String location("Location"), start_row("StartRow");

  // check if we need to move root range
  if (m_offload_servers.find(root_location) != m_offload_servers.end()) {
    String new_location = *locations_it;
    locations_it++;
    if (locations_it == locations.end())
      locations_it = locations.begin();

    RangeMoveSpecPtr move = new RangeMoveSpec(root_location.c_str(),
                new_location.c_str(), TableIdentifier::METADATA_ID,
                "", Key::END_ROOT_ROW);
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
        m_offload_servers.find(last_location) != m_offload_servers.end()) {
      size_t pos = last_key.find(':');
      HT_ASSERT(pos != string::npos);
      String table(last_key, 0, pos);
      String end_row(last_key, pos+1);
      String new_location = *locations_it;
      locations_it++;
      if (locations_it == locations.end())
        locations_it = locations.begin();

      RangeMoveSpecPtr move = new RangeMoveSpec(last_location.c_str(),
                                    new_location.c_str(),
                                    table.c_str(), last_start_row.c_str(),
                                    end_row.c_str());
      plan->moves.push_back(move);

      HT_DEBUG_OUT << table << "[" << last_start_row << ".." << end_row << "] "
          << last_location << "->" << new_location << HT_END;
    }
  }
}

