/* -*- c++ -*-
 * Copyright (C) 2007-2015 Hypertable, Inc.
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

#ifndef Hypertable_Master_RecoveryStepFuture_h
#define Hypertable_Master_RecoveryStepFuture_h

#include <Common/Time.h>
#include <Common/Timer.h>

#include <boost/thread/condition.hpp>

#include <condition_variable>
#include <map>
#include <memory>
#include <mutex>
#include <utility>
#include <vector>

namespace Hypertable {

  /**
   * Tracks outstanding RangeServer recover requests.
   */
  class RecoveryStepFuture {
  public:

    typedef std::map<String, std::pair<int32_t, String> > ErrorMapT;

    RecoveryStepFuture(const String &label, int plan_generation) :
      m_label(label), m_plan_generation(plan_generation),
      m_extend_timeout(false) { }

    void register_locations(StringSet &locations) {
      std::lock_guard<std::mutex> lock(m_mutex);
      for (auto &location : m_success)
        locations.erase(location);
      m_outstanding.clear();
      m_outstanding.insert(locations.begin(), locations.end());
      m_error_map.clear();
    }

    void status(const String &location, int plan_generation) {
      std::lock_guard<std::mutex> lock(m_mutex);
      m_extend_timeout = true;
      m_cond.notify_all();
    }

    void success(const String &location, int plan_generation) {
      std::lock_guard<std::mutex> lock(m_mutex);

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
      std::lock_guard<std::mutex> lock(m_mutex);

      if (plan_generation != m_plan_generation) {
        HT_INFOF("Ignoring response from %s for recovery step %s because "
                 "response plan generation %d does not match registered (%d)",
                 location.c_str(), m_label.c_str(), plan_generation,
                 m_plan_generation);
        return;
      }

      m_outstanding.erase(location);

      if (m_success.count(location) == 0)
        m_error_map[location] = make_pair(error, message);

      if (m_outstanding.empty())
        m_cond.notify_all();
      
    }

    bool wait_for_completion(uint32_t initial_timeout) {
      std::unique_lock<std::mutex> lock(m_mutex);
      ErrorMapT::iterator iter;

      auto expire_time = std::chrono::system_clock::now() +
        std::chrono::milliseconds(initial_timeout);

      while (!m_outstanding.empty()) {

        if (m_cond.wait_until(lock, expire_time) == std::cv_status::timeout) {
          if (!m_outstanding.empty()) {
            for (auto &location : m_outstanding) {
              iter = m_error_map.find(location);
              if (iter == m_error_map.end())
                m_error_map[location] = make_pair(Error::REQUEST_TIMEOUT, "");
            }
            m_outstanding.clear();
          }
        }
        if (m_extend_timeout) {
          expire_time = std::chrono::system_clock::now() +
            std::chrono::milliseconds(initial_timeout);
          m_extend_timeout = false;
        }
      }
      return m_error_map.empty();
    }

    void get_error_map(ErrorMapT &error_map) {
      std::lock_guard<std::mutex> lock(m_mutex);
      error_map = m_error_map;
    }

  protected:
    std::mutex m_mutex;
    std::condition_variable m_cond;
    String m_label;
    StringSet m_outstanding;
    StringSet m_success;
    ErrorMapT m_error_map;
    int m_plan_generation;
    bool m_extend_timeout;
  };

  typedef std::shared_ptr<RecoveryStepFuture> RecoveryStepFuturePtr;
}

#endif // Hypertable_Master_RecoveryStepFuture_h
