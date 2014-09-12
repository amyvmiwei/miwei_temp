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

/**
 * @file
 * The Stopwatch measures elapsed time.
 */

#ifndef HYPERTABLE_STOPWATCH_H
#define HYPERTABLE_STOPWATCH_H

#include <cstring>

#include "Time.h"

namespace Hypertable {

  /** @addtogroup Common
   *  @{
   */

  /**
   * The Stopwatch class measures elapsed time between instantiation (or a
   * call to @a start) and a call to @a stop.
   */
  class Stopwatch {
  public:
    /** Constructor; if @a start_running is true then the Stopwatch is started
     * immediately */
    Stopwatch(bool start_running = true)
      : m_running(false) {
      memset(&elapsed_time, 0, sizeof(elapsed_time));
      if (start_running)
        start();
    }

    /** Starts the Stopwatch */
    void start() {
      if (!m_running) {
        boost::xtime_get(&start_time, boost::TIME_UTC_);
        m_running = true;
      }
    }

    /** Stops the Stopwatch. Has no effect if the Stopwatch was not running. */
    void stop() {
      if (m_running) {
        boost::xtime stop_time;
        boost::xtime_get(&stop_time, boost::TIME_UTC_);
        if (start_time.sec == stop_time.sec)
          elapsed_time.nsec += stop_time.nsec - start_time.nsec;
        else {
          elapsed_time.sec += stop_time.sec - start_time.sec;
          elapsed_time.nsec += (1000000000L - start_time.nsec) + stop_time.nsec;
          if (elapsed_time.nsec > 1000000000L) {
            elapsed_time.sec += elapsed_time.nsec / 1000000000L;
            elapsed_time.nsec %= 1000000000L;
          }
        }
        m_running = false;
      }
    }

    /** Resets the Stopwatch */
    void reset() {
      memset(&elapsed_time, 0, sizeof(elapsed_time));
    }

    /** Returns the elapsed time. Can be called while the Stopwatch is running;
     * in this case the Stopwatch will continue to run. */
    double elapsed() {
      if (m_running) {
        stop();
        start();
      }
      return (double)elapsed_time.sec
            + ((double)elapsed_time.nsec / 1000000000.0);
    }

    /// Returns elapsed time in milliseconds.
    /// @return Elapsed time in milliseconds.
    int64_t elapsed_millis() {
      if (m_running) {
        stop();
        start();
      }
      return (elapsed_time.sec * 1000) + (elapsed_time.nsec / 1000000);
    }

  private:
    /** Flag whether the Stopwatch is currently running */
    bool m_running;

    /** The start time */
    boost::xtime start_time;

    /** The elapsed time */
    boost::xtime elapsed_time;
  };

  /** @} */

}

#endif // HYPERTABLE_STOPWATCH_H
