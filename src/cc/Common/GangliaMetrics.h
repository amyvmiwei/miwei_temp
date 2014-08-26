/* -*- c++ -*-
 * Copyright (C) 2007-2014 Hypertable, Inc.
 *
 * This file is part of Hypertable.
 *
 * Hypertable is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; version 3
 * of the License.
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
/// Declarations for GangliaMetrics.
/// This file contains type declarations for GangliaMetrics, a simple class for
/// aggregating metrics and sending them to the Ganglia gmond process running on
/// localhost.

#ifndef Common_GangliaMetrics_h
#define Common_GangliaMetrics_h

#include <Common/InetAddr.h>

#include <cstdint>
#include <memory>
#include <unordered_map>

namespace Hypertable {

  /// @addtogroup Common
  /// @{

  class GangliaMetrics {
  public:

    GangliaMetrics(uint16_t port);

    ~GangliaMetrics();

    void update(const std::string &name, const std::string &value);

    void update(const std::string &name, int16_t value);

    void update(const std::string &name, int32_t value);

    void update(const std::string &name, float value);

    void update(const std::string &name, double value);

    bool send();

    const char *get_error() { return m_error.c_str(); }

  private:
    bool connect();
    uint16_t m_port {};
    int m_sd {};
    std::string m_error;
    std::string m_message;
    std::unordered_map<std::string, std::string> m_values_string;
    std::unordered_map<std::string, int32_t> m_values_int;
    std::unordered_map<std::string, double> m_values_double;
    bool m_connected {};
  };

  /// Smart pointer to GangliaMetrics
  typedef std::shared_ptr<GangliaMetrics> GangliaMetricsPtr;

  /// @}
}

#endif // Common_GangliaMetrics_h
