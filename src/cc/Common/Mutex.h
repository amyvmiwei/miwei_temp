/* -*- c++ -*-
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
 * along with Hypertable. If not, see <http://www.gnu.org/licenses/>
 */

/** @file
 * Scoped lockers for recursive and non-recursive mutexes.
 * These helper classes use RAII to lock a mutex when they are constructed
 * and to unlock them when going out of scope.
 */

#ifndef Common_Mutex_h
#define Common_Mutex_h

#include <Common/Logger.h>

#include <atomic>
#include <mutex>

namespace Hypertable {

  /** @addtogroup Common
   *  @{
   */

  /** Mutex that maintains wait threads count.
   */
  class MutexWithStatistics {
  public:
    void lock() { if (m_enabled) m_count++; m_mutex.lock();}
    void unlock() { m_mutex.unlock(); if (m_enabled) m_count--; }
    int32_t get_waiting_threads() {return (int32_t)((m_enabled && m_count>1) ? m_count-1 : 0);}
    void set_statistics_enabled(bool val) { m_enabled = val; }
  private:
    std::atomic_int_fast32_t m_count {0};
    std::mutex m_mutex;
    bool m_enabled {true};
  };

  /** @} */

}

#endif // Common_Mutex_h
