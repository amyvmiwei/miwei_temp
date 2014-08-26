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
/// Declarations for metrics.
/// This file contains type declarations for metrics, a collection of classes to
/// aid in metrics collection.

#ifndef Common_metrics_h
#define Common_metrics_h

namespace Hypertable {

  /// @addtogroup Common
  /// @{

  template<typename _Tp> struct rate_metric {
    _Tp current {};
    _Tp previous {};
    void reset() { current=previous; }
    _Tp diff() { return current-previous; }
    float rate(float elapsed) { return (float)diff() / elapsed; }
  };

  /// @}
}

#endif // Common_metrics_h
