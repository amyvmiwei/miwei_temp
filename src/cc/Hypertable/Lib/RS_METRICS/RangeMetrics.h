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
/// Declarations for RangeMetrics.
/// This file contains declarations for RangeMetrics, a class for
/// aggregating metrics for an individual range.

#ifndef Hypertable_Lib_RS_METRICS_RangeMetrics_h
#define Hypertable_Lib_RS_METRICS_RangeMetrics_h

#include <Common/String.h>

#include <map>
#include <vector>

namespace Hypertable {
namespace Lib {
namespace RS_METRICS {

  /// @addtogroup libHypertable
  /// @{

  /// Single range metrics measurement.
  class RangeMeasurement {

  public:
    RangeMeasurement(const char *measurement, size_t len);
    RangeMeasurement() { memset(this, 0, sizeof(RangeMeasurement)); }
    void parse_measurement(const char *measurement, size_t len);

    int version;
    int64_t timestamp;
    int64_t disk_used;
    int64_t memory_used;
    double compression_ratio;
    double disk_byte_read_rate;
    double byte_write_rate;
    double byte_read_rate;
    double update_rate;
    double scan_rate;
    double cell_write_rate;
    double cell_read_rate;
  }; // RangeMeasurement


  /// Aggregates metrics for an individual range.
  class RangeMetrics {

  public:
    RangeMetrics(const char *server, const char *table_id, const char *end_row);

    void add_measurement(const char *measurement, size_t len);
    void set_start_row(const char *start_row, size_t len) {
      m_start_row = String(start_row, len);
      m_start_row_set = true;
    }
    void set_last_move(const char *move, size_t len);

    const String &get_server_id() const { return m_server_id; }
    const String &get_table_id() const { return m_table_id; }
    const String &get_start_row(bool *isset) const {
      *isset = m_start_row_set; return m_start_row;
    }
    const String &get_end_row() const { return m_end_row; }
    int64_t get_last_move(bool *isset) const {
      *isset = m_last_move_set;
      return m_last_move;
    }
    bool is_moveable() const;

    const std::vector<RangeMeasurement> &get_measurements() const { return m_measurements; }
    void get_avg_measurement(RangeMeasurement &measurement);

  private:
    std::vector<RangeMeasurement> m_measurements;
    String m_server_id;
    String m_table_id;
    String m_end_row;
    String m_start_row;
    int64_t m_last_move;
    bool m_start_row_set;
    bool m_last_move_set;
  }; // RangeMetrics

  typedef std::map<String, RangeMetrics> RangeMetricsMap;

  /// @}

} // namespace RS_METRICS
} // namespace Lib
} // namespace Hypertable

#endif // Hypertable_Lib_RS_METRICS_RangeMetrics_h
