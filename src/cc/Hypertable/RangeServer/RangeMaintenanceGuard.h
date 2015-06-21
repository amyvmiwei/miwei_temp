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

#ifndef Hypertable_RangeServer_RangeMaintenanceGuard_h
#define Hypertable_RangeServer_RangeMaintenanceGuard_h

#include <condition_variable>
#include <mutex>

namespace Hypertable {

  class RangeMaintenanceGuard {
  public:

    RangeMaintenanceGuard()  {}

    void activate() {
      std::lock_guard<std::mutex> lock(m_mutex);
      if (m_disabled)
        HT_THROW(Error::RANGESERVER_RANGE_NOT_ACTIVE, "");
      if (m_in_progress)
        HT_THROW(Error::RANGESERVER_RANGE_BUSY, "");
      m_in_progress = true;
    }

    void deactivate() {
      std::lock_guard<std::mutex> lock(m_mutex);
      m_in_progress = false;
      m_cond.notify_all();
    }

    void wait_for_complete(bool disable=false) {
      std::unique_lock<std::mutex> lock(m_mutex);
      m_cond.wait(lock, [this](){ return !m_in_progress; });
      if (disable)
        m_disabled = true;
    }

    bool in_progress() {
      std::lock_guard<std::mutex> lock(m_mutex);
      return m_in_progress;
    }

    void disable() {
      std::lock_guard<std::mutex> lock(m_mutex);
      m_disabled = true;
    }

    void enable() {
      std::lock_guard<std::mutex> lock(m_mutex);
      m_disabled = false;
    }

    class Activator {
    public:
      Activator(RangeMaintenanceGuard &guard) : m_guard(&guard) {
        m_guard->activate();
      }
      ~Activator() {
        m_guard->deactivate();
      }
    private:
      RangeMaintenanceGuard *m_guard;
    };

  private:
    std::mutex m_mutex;
    std::condition_variable m_cond;
    bool m_in_progress {};
    bool m_disabled {};
  };

}


#endif // Hypertable_RangeServer_RangeMaintenanceGuard_h
