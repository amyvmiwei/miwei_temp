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
/// Declarations for Metrics.
/// This file contains type declarations for Metrics, an abstract class that
/// defines the interface for metrics gathering classes.

#ifndef Common_Metrics_h
#define Common_Metrics_h

#include <Common/MetricsCollector.h>

#include <cstdint>

namespace Hypertable {

  /// @addtogroup Common
  /// @{

  /// Metrics interface.
  /// This abstract class defines the interface for classes that compute metrics
  /// and publish them to a metrics collector.
  class Metrics {
  public:

    /// Collects metrics.
    /// Computes metrics and publishes them via <code>collector</code>.
    /// @param now Current time in nanoseconds
    /// @param collector Metrics collector
    virtual void collect(int64_t now, MetricsCollector *collector) = 0;

  };

  /// @}
}

#endif // Common_Metrics_h
