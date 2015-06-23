/*
 * Copyright (C) 2007-2015 Hypertable, Inc.
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
/// Time related declarations.

#ifndef Common_Time_h
#define Common_Time_h

#include <iosfwd>

namespace Hypertable {

  /// @addtogroup Common
  /// @{

  /// Returns the current time in nanoseconds as a 64bit number
  int64_t get_ts64();

  /// Prints the current time as seconds and nanoseconds, delimited by '.'
  std::ostream &hires_ts(std::ostream &);

#if defined(__sun__)
  time_t timegm(struct tm *t);
#endif

  /// @}

}

#endif // Common_Time_h
