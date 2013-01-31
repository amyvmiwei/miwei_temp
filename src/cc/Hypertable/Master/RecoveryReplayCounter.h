/** -*- c++ -*-
 * Copyright (C) 2007-2012 Hypertable, Inc.
 *
 * This file is part of Hypertable.
 *
 * Hypertable is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; version 3 of the
 * License, or any later version.
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

#ifndef HYPERTABLE_RECOVERYREPLAYCOUNTER_H
#define HYPERTABLE_RECOVERYREPLAYCOUNTER_H

#include <boost/thread/condition.hpp>

#include <map>

#include "Common/ReferenceCount.h"
#include "Common/Time.h"
#include "Common/Timer.h"

namespace Hypertable {

  /**
   * Tracks outstanding RangeServer recover requests.
   */
  class RecoveryReplayCounter : public ReferenceCount {
  public:
    typedef std::map<uint32_t, int> ErrorMap;

    RecoveryReplayCounter(int plan_generation)
      : m_plan_generation(plan_generation), m_outstanding(0),
        m_done(false), m_errors(false), m_timed_out(false) { }

    void add(size_t num) {
      ScopedLock lock(m_mutex);
      m_outstanding += num;
    }

    bool complete(int plan_generation, int32_t error, const ErrorMap &error_map) {
      ScopedLock lock(m_mutex);
      HT_ASSERT(m_outstanding >= error_map.size());
      m_outstanding -= error_map.size();

      if (attempt != m_attempt)
        return false;

      foreach_ht (const ErrorMap::value_type &vv, error_map) {
        m_error_map[vv.first] = vv.second;
        if (vv.second != Error::OK)  {
          HT_WARN_OUT << "Received error " << vv.second << " for fragment "
              << vv.first << HT_END;
          m_errors = true;
        }
      }
      if (m_outstanding == 0) {
        m_done = true;
        m_cond.notify_all();
      }
      return success;
    }

    bool wait_for_completion(Timer &timer) {
      ScopedLock lock(m_mutex);
      boost::xtime expire_time;

      timer.start();

      while (m_outstanding) {
        boost::xtime_get(&expire_time, boost::TIME_UTC_);
        xtime_add_millis(expire_time, timer.remaining());
        if (!m_cond.timed_wait(lock, expire_time)) {
          HT_WARN_OUT << "RecoveryReplayCounter timed out" << HT_END;
          m_errors = true;
          m_timed_out = true;
          --m_outstanding;
        }
      }
      m_done = true;
      return !(m_errors);
    }

    void set_errors(const std::vector<uint32_t> &fragments, int error) {
      ScopedLock lock(m_mutex);
      m_outstanding -= fragments.size();
      foreach_ht(uint32_t fragment, fragments) {
        m_error_map[fragment] = error;
      }
      if (m_outstanding == 0) {
        m_done = true;
        m_cond.notify_all();
      }
    }

    bool has_errors() {
      ScopedLock lock(m_mutex);
      return m_errors;
    }

    const ErrorMap &get_errors() {
      ScopedLock lock(m_mutex);
      return m_error_map;
    }

    bool timed_out() {
      ScopedLock lock(m_mutex);
      return m_timed_out;
    }

    uint32_t get_attempt() const { return m_attempt; }

    bool has_errors() const { return m_errors; }

  protected:
    Mutex m_mutex;
    boost::condition m_cond;
    int m_attempt;
    size_t m_outstanding;
    bool m_done;
    bool m_errors;
    bool m_timed_out;
    ErrorMap m_error_map;
  };
  typedef intrusive_ptr<RecoveryReplayCounter> RecoveryReplayCounterPtr;
}

#endif // HYPERTABLE_RECOVERYREPLAYCOUNTER_H
