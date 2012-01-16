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

#ifndef HYPERTABLE_SERVERMETRICS_H
#define HYPERTABLE_SERVERMETRICS_H

#include <vector>
#include <set>
#include "Common/StringExt.h"

namespace Hypertable {

  using namespace std;

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
  }; // ServerMeasurement

  class ServerMetrics {

  public:

    ServerMetrics(const String &id) : m_id(id) { }
    ServerMetrics(const char *id) : m_id(id) { }

    void add_measurement(const char *measurement, size_t len);
    const vector<ServerMeasurement> &get_measurements() const { return m_measurements; }

    const String &get_id() const { return m_id; }
    String &get_id() { return m_id; }

  private:
    vector<ServerMeasurement> m_measurements;
    set<String> m_ranges;
    String m_id;
  }; // ServerMetrics

} // namespace Hypertable

#endif // HYPERTABLE_SERVERMETRICS_H
