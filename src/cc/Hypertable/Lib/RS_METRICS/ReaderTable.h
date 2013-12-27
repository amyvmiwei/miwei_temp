/* -*- c++ -*-
 * Copyright (C) 2007-2013 Hypertable, Inc.
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

/// @file
/// Declarations for ReaderTable.
/// This file contains declarations for ReaderTable, a derived
/// Reader class for reading %RangeServer metrics from the
/// <code>sys/RS_METRICS</code> table.

#ifndef Hypertable_Lib_RS_METRICS_ReaderTable_H
#define Hypertable_Lib_RS_METRICS_ReaderTable_H

#include "Reader.h"

#include <Hypertable/Lib/KeySpec.h>
#include <Hypertable/Lib/Table.h>

namespace Hypertable {
namespace Lib {
namespace RS_METRICS {

  // This class reads in data from the sys/RS_METRICS table and makes it accessible.
  // It reads the data either from the sys/RS_METRICS table or from a text file dump if the same

  /// @addtogroup libHypertable
  /// @{

  /// Reads metrics from the <code>sys/RS_METRICS</code> table.
  class ReaderTable : public Reader {
  public:
    ReaderTable(TablePtr &rs_metrics) : m_table(rs_metrics) { }
    virtual ~ReaderTable() { }
    virtual void get_range_metrics(const char *server_id,
                                   RangeMetricsMap &range_metrics);
    virtual void get_server_metrics(vector<ServerMetrics> &server_metrics);

  private:

    void parse_cell(const KeySpec &key, const char *value, size_t value_len);
    TablePtr m_table;
  };

  /// @}

} // namespace RS_METRICS
} // namespace Lib
} // namespace Hypertable

#endif // Hypertable_Lib_RS_METRICS_ReaderTable_H
