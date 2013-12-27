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
/// Declarations for Reader.
/// This file contains declarations for Reader, an interface class
/// that facilitates the reading of %RangeServer metrics data from an arbitrary
/// source.

#ifndef Hypertable_Lib_RS_METRICS_Reader_H
#define Hypertable_Lib_RS_METRICS_Reader_H

#include <Hypertable/Lib/RS_METRICS/RangeMetrics.h>
#include <Hypertable/Lib/RS_METRICS/ServerMetrics.h>

#include <Common/String.h>

#include <vector>

namespace Hypertable {
namespace Lib {
namespace RS_METRICS {

  /// @addtogroup libHypertable
  /// @{

  /// Interface class for reading %RangeServer metrics.
  class Reader {
  public:
    virtual ~Reader() { }
    virtual void get_range_metrics(const char *server_id,
                                   RangeMetricsMap &range_metrics) = 0;
    virtual void get_server_metrics(std::vector<ServerMetrics> &server_metrics) = 0;
  };

  /// @}

} // namespace RS_METRICS
} // namespace Lib
} // namespace Hypertable

#endif // Hypertable_Lib_RS_METRICS_Reader_H
