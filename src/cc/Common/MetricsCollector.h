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
/// This file contains type declarations for MetricsCollector, a simple class for
/// aggregating metrics and sending them to the Ganglia gmond process running on
/// localhost.

#ifndef Common_MetricsCollector_h
#define Common_MetricsCollector_h

#include <cstdint>
#include <string>

namespace Hypertable {

  /// @addtogroup Common
  /// @{

  class MetricsCollector {
  public:

    virtual void update(const std::string &name, const std::string &value) = 0;

    virtual void update(const std::string &name, int16_t value) = 0;

    virtual void update(const std::string &name, int32_t value) = 0;

    virtual void update(const std::string &name, float value) = 0;

    virtual void update(const std::string &name, double value) = 0;

    virtual void publish() = 0;

  };

  /// @}
}

#endif // Common_MetricsCollector_h
