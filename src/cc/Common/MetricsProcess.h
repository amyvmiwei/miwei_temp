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
/// This file contains type declarations for MetricsProcess, a simple class for
/// aggregating metrics and sending them to the Ganglia gmond process running on
/// localhost.

#ifndef Common_MetricsProcess_h
#define Common_MetricsProcess_h

#include <Common/Metrics.h>
#include <Common/MetricsCollector.h>

#include <cstdint>

namespace Hypertable {

  /// @addtogroup Common
  /// @{

  class MetricsProcess : public Metrics {
  public:
    MetricsProcess();
    void collect(int64_t now, MetricsCollector *collector) override;
  private:
    int64_t m_last_timestamp {};
    int64_t m_last_sys {};
    int64_t m_last_user {};
  };

  /// @}
}

#endif // Common_MetricsProcess_h
