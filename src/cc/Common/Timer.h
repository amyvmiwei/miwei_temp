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
 * A timer class to keep timeout states across AsyncComm related calls.
 */

#ifndef HYPERTABLE_TIMER_H
#define HYPERTABLE_TIMER_H

#include <cstring>

#include "Logger.h"
#include "Time.h"

namespace Hypertable {

  /** @addtogroup Common
   *  @{
   */

  /**
   * A timer class to keep timeout states across AsyncComm related calls.
   */
  class Timer {
  public:
    /**
     * Constructor; assigns number of milliseconds after which the timer will
     * expire
     *
     * @param millis Number of milliseconds after timer will expire
     * @param start_timer If true, timer is started immediately; otherwise
     *      start with %start
     */
    Timer(uint32_t millis, bool start_timer = false)
      : m_running(false), m_started(false),
        m_duration(millis), m_remaining(millis) {
      if (start_timer)
        start();
    }

    /**
     * Assignment operator copies state but does not start immediately
     *
     * @param src Reference to the other timer object which is copied
     */
    Timer& operator=(Timer &src) {
      if (&src != this) {
        m_running = false;
        m_started = false;
        m_duration = src.duration();
        m_remaining = src.remaining();
      }
      return *this;
    }

    /**
     * Starts the timer. Will have no effect if the timer is still running.
     */
    void start() {
      if (!m_running) {
        boost::xtime_get(&start_time, boost::TIME_UTC_);
        m_running = true;
        if (!m_started)
          m_started = true;
      }
    }

    /**
     * Stops the timer. Will assert that the timer was started. Updates the
     * remaining time (see %remaining).
     */
    void stop() {
      boost::xtime stop_time;
      boost::xtime_get(&stop_time, boost::TIME_UTC_);
      uint32_t adjustment;

      assert(m_started);

      if (start_time.sec == stop_time.sec) {
        adjustment = (stop_time.nsec - start_time.nsec) / 1000000;
        m_remaining = (adjustment < m_remaining) ? m_remaining - adjustment : 0;
      }
      else {
        adjustment = ((stop_time.sec - start_time.sec) - 1) * 1000;
        m_remaining = (adjustment < m_remaining) ? m_remaining - adjustment : 0;
        adjustment = ((1000000000 - start_time.nsec) + stop_time.nsec)
                      / 1000000;
        m_remaining = (adjustment < m_remaining) ? m_remaining - adjustment : 0;
      }
      m_running = false;
    }

    /**
     * Resets the timer
     */
    void reset(bool start_timer = false) {
      m_running = false;
      m_started = false;
      m_remaining = m_duration;
      if (start_timer)
        start();
    }

    /**
     * Returns the remaining time till expiry
     */
    uint32_t remaining() {
      if (m_running) {
        stop();
        start();
      }
      return m_remaining;
    }

    /**
     * Returns true if the timer is expired
     */
    bool expired() {
      return remaining() == 0;
    }

    /**
     * Returns true if the timer is still running (not yet expired
     */
    bool is_running() {
      return m_running;
    }

    /**
     * Returns the duration of the timer
     */
    uint32_t duration() {
      return m_duration;
    }

  private:
    /** The time when the timer was started */
    boost::xtime start_time;

    /** True if the timer is running */
    bool m_running;

    /** True if the timer was started */
    bool m_started;

    /** The duration of the timer (in milliseconds) */
    uint32_t m_duration;

    /** The remaining time till expiration (in milliseconds) */
    uint32_t m_remaining;
  };

  /** @} */

}

#endif // HYPERTABLE_TIMER_H
