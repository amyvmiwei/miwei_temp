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
/// Declarations for MetricsCollector.
/// This file contains type declarations for MetricsCollector, an abstract class
/// that defines an interface for metrics collectors.

#ifndef Common_MetricsCollector_h
#define Common_MetricsCollector_h

#include <cstdint>
#include <string>

namespace Hypertable {

  /// @addtogroup Common
  /// @{

  /// Abstract metrics collector
  class MetricsCollector {
  public:

    /// Updates string metric value.
    /// @param name Relative name of metric
    /// @param value Metric value
    virtual void update(const std::string &name, const std::string &value) = 0;

    /// Updates int16 metric value.
    /// @param name Relative name of metric
    /// @param value Metric value
    virtual void update(const std::string &name, int16_t value) = 0;

    /// Updates int32 metric value.
    /// @param name Relative name of metric
    /// @param value Metric value
    virtual void update(const std::string &name, int32_t value) = 0;

    /// Updates float metric value.
    /// @param name Relative name of metric
    /// @param value Metric value
    virtual void update(const std::string &name, float value) = 0;

    /// Updates double metric value.
    /// @param name Relative name of metric
    /// @param value Metric value
    virtual void update(const std::string &name, double value) = 0;

    /// Publishes collected metrics.
    virtual void publish() = 0;

  };

  /// @}
}

#endif // Common_MetricsCollector_h
