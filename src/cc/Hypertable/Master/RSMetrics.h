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

#ifndef HYPERTABLE_RSMETRICS_H
#define HYPERTABLE_RSMETRICS_H

#include <vector>

#include "Common/String.h"
#include "Common/ReferenceCount.h"

#include "Hypertable/Lib/Client.h"

#include "RangeMetrics.h"
#include "ServerMetrics.h"

namespace Hypertable {

  using namespace std;

  // This class reads in data from the sys/RS_METRICS table and makes it accessible.
  // It reads the data either from the sys/RS_METRICS table or from a text file dump if the same

  class RSMetrics : public ReferenceCount {
  public:
    RSMetrics(TablePtr &rs_metrics) : m_table(rs_metrics) { }
    void get_range_metrics(const char *server_id, RangeMetricsMap &range_metrics);
    void get_server_metrics(vector<ServerMetrics> &server_metrics);

  private:

    void parse_cell(const KeySpec &key, const char *value, size_t value_len);
    TablePtr m_table;
  }; // RSMetrics
  typedef intrusive_ptr<RSMetrics> RSMetricsPtr;

} // namespace Hypertable

#endif // HYPERTABLE_RSMETRICS_H
