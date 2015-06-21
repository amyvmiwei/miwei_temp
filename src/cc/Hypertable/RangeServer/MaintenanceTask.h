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

#ifndef Hypertable_RangeServer_MaintenanceTask_h
#define Hypertable_RangeServer_MaintenanceTask_h

#include "Common/Time.h"

#include "MaintenanceFlag.h"
#include "Range.h"

#include <chrono>

namespace Hypertable {

  class MaintenanceTask {
  public:

    MaintenanceTask(uint32_t _level, int _priority,
                    std::chrono::time_point<std::chrono::steady_clock> &stime,
                    RangePtr &range, const String &desc)
      : start_time(stime), level(_level), priority(_priority), m_range(range),
        m_description(desc) { }

    MaintenanceTask(uint32_t _level, int _priority, const String &desc) :
      level(_level), priority(_priority), m_description(desc) {
      start_time = std::chrono::steady_clock::now();
    }

    virtual ~MaintenanceTask() { }

    virtual void execute() = 0;

    String &description() { return m_description; }

    bool retry() { return m_retry; }
    void set_retry(bool retry) { m_retry = retry; }

    uint32_t get_retry_delay() { return m_retry_delay_millis; }
    void set_retry_delay(uint32_t delay) { m_retry_delay_millis = delay; }

    int get_priority() { return priority; }
    void set_priority(int p) { priority = p; }

    Range *get_range() { return m_range.get(); }

    void add_subtask(const void *obj, int flags) {
      m_map[obj] = flags;
    }

    std::chrono::time_point<std::chrono::steady_clock> start_time;
    uint32_t level;
    int priority;

  protected:
    RangePtr m_range;
    MaintenanceFlag::Map m_map;

  private:
    bool m_retry {};
    uint32_t m_retry_delay_millis;
    String m_description;
  };

}

#endif // Hypertable_RangeServer_MaintenanceTask_h
