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

#ifndef HYPERTABLE_RECOVERYSTEPFUTURE_H
#define HYPERTABLE_RECOVERYSTEPFUTURE_H

#include <boost/thread/condition.hpp>

#include <set>
#include <utility>
#include <vector>

#include "Common/ReferenceCount.h"
#include "Common/Time.h"
#include "Common/Timer.h"

namespace Hypertable {

  /**
   * Tracks outstanding RangeServer recover requests.
   */
  class RecoveryStepFuture : public ReferenceCount {
  public:

    typedef std::map<const String, std::pair<int32_t, String> > ErrorMapT;

    RecoveryStepFuture(const String &label, int plan_generation) :
      m_label(label), m_plan_generation(plan_generation) { }

    void register_locations(StringSet &locations) {
      ScopedLock lock(m_mutex);
      foreach_ht (const String &location, m_success)
        locations.erase(location);
      m_outstanding.clear();
      m_outstanding.insert(locations.begin(), locations.end());
      m_error_map.clear();
    }

    void success(const String &location, int plan_generation) {
      ScopedLock lock(m_mutex);

      if (m_outstanding.empty()) {
        m_cond.notify_all();
        return;
      }

      m_success.insert(location);

      m_outstanding.erase(location);

      // purge from error map
      ErrorMapT::iterator iter = m_error_map.find(location);
      if (iter != m_error_map.end())
        m_error_map.erase(iter);

      if (m_outstanding.empty())
        m_cond.notify_all();
    }

    void failure(const String &location, int plan_generation,
                 int32_t error, const String &message) {
      ScopedLock lock(m_mutex);

      if (plan_generation != m_plan_generation) {
        HT_INFOF("Ignoring response from %s for recovery step %s because "
                 "response plan generation %d does not match registered (%d)",
                 location.c_str(), m_label.c_str(), plan_generation,
                 m_plan_generation);
        return;
      }

      ErrorMapT::iterator iter = m_error_map.find(location);

      m_outstanding.erase(location);

      if (m_success.count(location) == 0)
        m_error_map[location] = make_pair(error, message);

      if (m_outstanding.empty())
        m_cond.notify_all();
      
    }

    bool wait_for_completion(Timer &timer) {
      ScopedLock lock(m_mutex);
      boost::xtime expire_time;
      ErrorMapT::iterator iter;

      timer.start();

      while (!m_outstanding.empty()) {
        boost::xtime_get(&expire_time, boost::TIME_UTC_);
        xtime_add_millis(expire_time, timer.remaining());
        if (!m_cond.timed_wait(lock, expire_time)) {
          if (!m_outstanding.empty()) {
            foreach_ht (const String &location, m_outstanding) {
              iter = m_error_map.find(location);
              if (iter == m_error_map.end())
                m_error_map[location] = make_pair(Error::REQUEST_TIMEOUT, "");
            }
            m_outstanding.clear();
          }
        }
      }
      return m_error_map.empty();
    }

    void get_error_map(ErrorMapT &error_map) {
      ScopedLock lock(m_mutex);
      error_map = m_error_map;
    }

  protected:
    Mutex m_mutex;
    boost::condition m_cond;
    String m_label;
    StringSet m_outstanding;
    StringSet m_success;
    ErrorMapT m_error_map;
    int m_plan_generation;
  };
  typedef intrusive_ptr<RecoveryStepFuture> RecoveryStepFuturePtr;
}

#endif // HYPERTABLE_RECOVERYSTEPFUTURE_H
