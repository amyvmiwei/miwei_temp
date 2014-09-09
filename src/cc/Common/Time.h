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
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
 * 02110-1301, USA.
 */

/** @file
 * High resolution time handling based on boost::xtime.
 */

#ifndef HYPERTABLE_HIRES_TIME_H
#define HYPERTABLE_HIRES_TIME_H

#include <iosfwd>
#include <boost/version.hpp>
#include <boost/thread/xtime.hpp>

#if BOOST_VERSION < 105000
namespace boost {
  enum {
    TIME_UTC_ = 1
  };
}
#endif

namespace Hypertable {

  /** @addtogroup Common
   *  @{
   */

  /**
   * Adds milliseconds to a timestamp
   *
   * @param xt The current timestamp
   * @param millis The milliseconds that are added
   */ 
  bool xtime_add_millis(boost::xtime &xt, uint32_t millis);

  /**
   * Substracts milliseconds from a timestamp
   *
   * @param xt The current timestamp
   * @param millis The milliseconds that are substracted
   */ 
  bool xtime_sub_millis(boost::xtime &xt, uint32_t millis);

  /**
   * Returns the difference between two timestamps - an "early" and a "late"
   * timestamp. Returns 0 if the "early" timestamp is older than the "late"
   * one.
   *
   * @param early The earlier (older) timestamp
   * @param late The later (newer) timestamp
   * @return Millisecond difference between <code>early</code> and <code>late</code>
   */
  int64_t xtime_diff_millis(boost::xtime &early, boost::xtime &late);

  using boost::TIME_UTC_;

  /**
   * High-Resolution Timestamp based on boost::xtime with some additional
   * helper functions to make it more convenient
   */
  struct HiResTime : public boost::xtime {
    /**
     * Constructor; initializes object with the current point in time as a
     * duration since the epoch specified by clock_type.
     */
    HiResTime(int clock_type = TIME_UTC_) {
      reset(clock_type);
    }

    /**
     * Initializes object with the current point in time as a duration
     * since the epoch specified by clock_type.
     *
     * @param clock_type Requested clock type (see boost::xtime documentation)
     * @return true on success; otherwise false
     */
    bool reset(int clock_type = TIME_UTC_) {
      return boost::xtime_get(this, clock_type) != 0;
    }

    /**
     * Compares this object with other HiResTime; returns -1 if this one is
     * lower (earlier), 0 if both are equal or +1 if this one is greater
     * (later) than the other HiResTime
     *
     * @param other Reference to the other HiResTime object
     * @return -1/0/+1 if this object is older/equal/newer than @a other
     */
    int cmp(const HiResTime &other) const {
      return boost::xtime_cmp(*this, other);
    }

    /**
     * operator< based on %cmp; returns true if this timestamp is lower than
     * the other timestamp
     */
    bool operator<(const HiResTime &other) const {
      return cmp(other) < 0;
    }

    /**
     * operator< based on %cmp; returns true if this timestamp is equal to
     * the other timestamp
     */
    bool operator==(const HiResTime &other) const {
      return cmp(other) == 0;
    }

    /** Adds %ms milliseconds to the current timestamp */
    HiResTime &operator+=(uint32_t ms) {
      xtime_add_millis(*this, ms);
      return *this;
    }

    /** Substracts %ms milliseconds from the current timestamp */
    HiResTime &operator-=(uint32_t ms) {
      xtime_sub_millis(*this, ms);
      return *this;
    }
  };

  /**
   * Returns the current time in nanoseconds as a 64bit number
   */
  int64_t get_ts64();

  /** Prints the current time as seconds and nanoseconds, delimited by '.' */
  std::ostream &hires_ts(std::ostream &);

  /** Prints the current date in the format YYYY-MM-DD hh:mm:ss.nnnn */
  std::ostream &hires_ts_date(std::ostream &);

#if defined(__sun__)
  time_t timegm(struct tm *t);
#endif

  /** @} */

} // namespace Hypertable

#endif // HYPERTABLE_HIRES_TIME_H
