/*
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
 * along with Hypertable. If not, see <http://www.gnu.org/licenses/>
 */

/** @file
 * An inline helper function to parse timestamps in
 * YYYY-mm-dd[ HH:MM[:SS[.SS|:NS]]] format
 */

#ifndef HYPERTABLE_TIMEINLINE_H
#define HYPERTABLE_TIMEINLINE_H

#include <Common/System.h>
#include <Common/Time.h>

#include <stdexcept>

namespace Hypertable {

/** @addtogroup Common
 *  @{
 */

#ifndef HT_DELIM_CHECK
# define HT_DELIM_CHECK(_l_, _c_) do { \
    if ((_l_) != (_c_)) \
      throw std::runtime_error("expected " #_l_ " to be " #_c_); \
  } while (0)
#endif

#ifndef HT_RANGE_CHECK
# define HT_RANGE_CHECK(_v_, _min_, _max_) do { \
    if ((_v_) < (_min_) || (_v_) > (_max_)) \
      throw std::range_error(#_v_ " must be between " #_min_ " and " #_max_); \
  } while (0)
#endif

/**
 * Inline function which parses a string with a timestamp in localtime and
 * returns a 64bit nanosecond timestamp
 *
 * @param ts c-string with the timestamp formatted as
 *      YYYY-mm-dd[ HH:MM[:SS[.SS|:NS]]]
 * @return Timestamp in nanoseconds
 * @throw std::range_error if a range (i.e. 0..59) is exceeded
 * @throw std::runtime_error if a delimiter is missing
 */
inline int64_t
parse_ts(const char *ts) {
  const int64_t G = 1000000000LL;
  tm tv;
  char *last;
  int64_t ns=0;

  System::initialize_tm(&tv);
  tv.tm_year = strtol(ts, &last, 10) - 1900;
  HT_DELIM_CHECK(*last, '-');
  tv.tm_mon = strtol(last + 1, &last, 10) - 1;
  HT_RANGE_CHECK(tv.tm_mon, 0, 11);
  HT_DELIM_CHECK(*last, '-');
  tv.tm_mday = strtol(last + 1, &last, 10);
  HT_RANGE_CHECK(tv.tm_mday, 1, 31);

  if (*last == 0)
    return mktime(&tv) * G;

  HT_DELIM_CHECK(*last, ' ');
  tv.tm_hour = strtol(last + 1, &last, 10);
  HT_RANGE_CHECK(tv.tm_hour, 0, 23);
  HT_DELIM_CHECK(*last, ':');
  tv.tm_min = strtol(last + 1, &last, 10);
  HT_RANGE_CHECK(tv.tm_min, 0, 59);

  if (*last == 0)
    return mktime(&tv) * G;

  HT_DELIM_CHECK(*last, ':');

  tv.tm_sec = strtol(last + 1, &last, 10);
  HT_RANGE_CHECK(tv.tm_sec, 0, 59);

  // nanoseconds
  if (*last == ':')
    ns = strtol(last+1, &last, 10);
  else if (*last == '.')
    ns = (int64_t)(strtod(last, 0) * 1000000000.0);

  return (int64_t)(mktime(&tv) * G + ns);
}

/** @} */

} // namespace Hypertable

#endif /* HYPERTABLE_TIMEINLINE_H */
