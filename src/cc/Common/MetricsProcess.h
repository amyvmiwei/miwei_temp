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
/// Declarations for MetricsProcess.
/// This file contains type declarations for MetricsProcess, a class for
/// computing general process metrics.

#ifndef Common_MetricsProcess_h
#define Common_MetricsProcess_h

#include <Common/Metrics.h>
#include <Common/MetricsCollector.h>

#include <cstdint>

namespace Hypertable {

  /// @addtogroup Common
  /// @{

  /// Computes and publishes general process metrics.
  class MetricsProcess : public Metrics {
  public:

    /// Constructor.
    /// Initializes m_last_sys and m_last_user from data gathered from SIGAR.
    /// Initializes m_last_timestamp to current time in milliseconds.
    MetricsProcess();

    /// Collects process metrics.
    /// Computes process metrics and publishes them via <code>collector</code>.
    /// The following JVM metrics and process metrics collected from SIGAR are
    /// computed and published:
    /// <ul>
    /// <li> <code>cpu.sys</code> - CPU system time (%)</li>
    /// <li> <code>cpu.user</code> - CPU user time (%)</li>
    /// <li> <code>memory.virtual</code> - Virtual memory (GB)</li>
    /// <li> <code>memory.resident</code> - Resident memory (GB)</li>
    /// <li> <code>memory.heap</code> - Heap size (GB)</li>
    /// <li> <code>memory.heapSlack</code> - Heap slack bytes (GB)</li>
    /// <li> <code>version</code> - Hypertable version string</li>
    /// </ul>
    /// @param now Current time in nanoseconds
    /// @param collector Metrics collector
    void collect(int64_t now, MetricsCollector *collector) override;

  private:

    /// Last collection timestamp in nanoseconds
    int64_t m_last_timestamp {};

    /// Last recorded CPU system time
    int64_t m_last_sys {};

    /// Last recorded CPU user time
    int64_t m_last_user {};
  };

  /// @}
}

#endif // Common_MetricsProcess_h
