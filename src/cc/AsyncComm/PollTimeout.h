/*
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

/** @file
 * Declarations for PollTimeout.
 * This file contains type declarations for PollTimeout, a class used to track
 * the next polling loop timeout.
 */

#ifndef HYPERTABLE_POLLTIMEOUT_H
#define HYPERTABLE_POLLTIMEOUT_H


#include <boost/thread/xtime.hpp>

#include <cassert>

namespace Hypertable {

  /** @addtogroup AsyncComm
   *  @{
   */

  /** Maintains next timeout for event polling loop.  This class is used to
   * maintain and provide access to the next timeout for the event polling
   * loops.  It contains accessor methods to return the timeout in different
   * formats required by the various polling interfaces.
   */
  class PollTimeout {

  public:

    /** Constructor. */
    PollTimeout() : ts_ptr(0), duration_millis(-1) { return; }

    /** Sets the next timeout.
     * @param now Current time
     * @param expire Absolute time of next timeout
     */
    void set(boost::xtime &now, boost::xtime &expire) {
      assert((xtime_cmp(now , expire) <= 0));
      if (now.sec == expire.sec) {
        duration_ts.tv_sec = 0;
        duration_ts.tv_nsec = expire.nsec - now.nsec;
      }
      else {
        uint64_t nanos = expire.nsec + (1000000000LL - now.nsec);
        duration_ts.tv_sec = ((expire.sec - now.sec) - 1)
                              + (nanos / 1000000000LL);
        duration_ts.tv_nsec = nanos % 1000000000LL;
      }
      ts_ptr = &duration_ts;
      duration_millis = (int)((duration_ts.tv_sec * 1000)
                              + (duration_ts.tv_nsec / 1000000));
      assert(duration_millis >= 0);
    }

    /** Sets the next timeout to be an indefinite time in the future.
     */
    void set_indefinite() {
      ts_ptr = 0;
      duration_millis = -1;
    }

    /** Gets duration until next timeout in the form of milliseconds.
     * @return Milliseconds until next timeout
     */
    int get_millis() { return duration_millis; }

    /** Gets duration until next timeout in the form of a pointer to timespec.
     * @return Pointer to timespec representing duration until next timeout.
     */
    struct timespec *get_timespec() { return ts_ptr; }

  private:

    /// Pointer to to #duration_ts or 0 if indefinite
    struct timespec *ts_ptr;

    /// timespec structure holding duration until next timeout
    struct timespec duration_ts;

    /// Duration until next timeout in milliseconds
    int duration_millis;
  };
  /** @}*/
}

#endif // HYPERTABLE_POLLTIMEOUT_H
