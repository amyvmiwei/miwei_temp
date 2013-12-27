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
/// Declarations for ServerMetrics.
/// This file contains declarations for ServerMetrics, a class for
/// aggregating metrics for an individual %RangeServer.

#ifndef Hypertable_Lib_RS_METRICS_ServerMetrics_h
#define Hypertable_Lib_RS_METRICS_ServerMetrics_h

#include <Common/StringExt.h>

#include <vector>
#include <set>

namespace Hypertable {
namespace Lib {
namespace RS_METRICS {

  /// @addtogroup libHypertable
  /// @{

  /// Single server metrics measurement.
  class ServerMeasurement {

  public:

    ServerMeasurement() { memset(this, 0, sizeof(ServerMeasurement)); }
    ServerMeasurement(const char *measurement, size_t len);
    void parse_measurement(const char *measurement, size_t len);

    int version;
    int64_t timestamp;
    double loadavg;
    double disk_bytes_read_rate;
    double bytes_written_rate;
    double bytes_scanned_rate;
    double updates_rate;
    double scans_rate;
    double cells_written_rate;
    double cells_scanned_rate;
    double page_in;
    double page_out;
    int64_t disk_total;
    int64_t disk_avail;
  };

  /// Aggregates metrics for an individual %RangeServer.
  class ServerMetrics {

  public:

    ServerMetrics(const String &id) : m_id(id) { }
    ServerMetrics(const char *id) : m_id(id) { }

    void add_measurement(const char *measurement, size_t len);
    const std::vector<ServerMeasurement> &get_measurements() const { return m_measurements; }

    const String &get_id() const { return m_id; }
    String &get_id() { return m_id; }

  private:
    std::vector<ServerMeasurement> m_measurements;
    std::set<String> m_ranges;
    String m_id;
  }; // ServerMetrics

  /// @}

} // namespace RS_METRICS
} // namespace Lib
} // namespace Hypertable

#endif // Hypertable_Lib_RS_METRICS_ServerMetrics_h
