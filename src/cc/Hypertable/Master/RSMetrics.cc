/** -*- c++ -*-
 * Copyright (C) 2007-2012 Hypertable, Inc.
 *
 * This file is part of Hypertable.
 *
 * Hypertable is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 3
 * of the License, or any later version.
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

#include "Common/Logger.h"

#include <map>
#include <set>

#include "RSMetrics.h"
#include "Hypertable/Lib/LoadDataSourceFactory.h"

using namespace std;
using namespace Hypertable;

void RSMetrics::get_range_metrics(const char *server, RangeMetricsMap &range_metrics) {
  ScanSpec scan_spec;
  RowInterval ri;
  Cell cell;
  String scan_start_row = format("%s:", server);
  String scan_end_row = format("%s::", server);
  const char *table, *end_row;

  ri.start = scan_start_row.c_str();
  ri.end = scan_end_row.c_str();
  ri.end_inclusive = false;
  scan_spec.row_intervals.push_back(ri);
  scan_spec.columns.push_back("range");
  scan_spec.columns.push_back("range_start_row");
  scan_spec.columns.push_back("range_move");

  TableScannerPtr scanner = m_table->create_scanner(scan_spec);
  while (scanner->next(cell)) {
    table = strchr(cell.row_key, ':') + 1;
    end_row = cell.column_qualifier;
    HT_ASSERT(!(table == NULL || end_row == NULL));
    String key = format("%s:%s", table, end_row);

    RangeMetricsMap::iterator rm_it = range_metrics.find(key);

    if (rm_it == range_metrics.end()) {
      RangeMetrics rm(server, table, end_row);
      pair<RangeMetricsMap::iterator, bool>  ret = range_metrics.insert(make_pair(key, rm));
      rm_it = ret.first;
    }

    if (!strcmp(cell.column_family, "range"))
      rm_it->second.add_measurement((const char*) cell.value, cell.value_len);
    else if (!strcmp(cell.column_family, "range_start_row"))
      rm_it->second.set_start_row((const char*) cell.value, cell.value_len);
    else if (!strcmp(cell.column_family, "range_move"))
      rm_it->second.set_last_move((const char*) cell.value, cell.value_len);
  }
}

void RSMetrics::get_server_metrics(vector<ServerMetrics> &server_metrics) {
  ScanSpec scan_spec;
  scan_spec.columns.push_back("server");

  TableScannerPtr scanner = m_table->create_scanner(scan_spec);
  Cell cell;
  bool empty=true;

  while (scanner->next(cell)) {
    HT_ASSERT(!strcmp(cell.column_family,"server"));
    if (empty || server_metrics.back().get_id() != cell.row_key) {
      empty = false;
      ServerMetrics sm(cell.row_key);
      server_metrics.push_back(sm);
    }
    server_metrics.back().add_measurement((const char*)cell.value, cell.value_len);
  }
}
