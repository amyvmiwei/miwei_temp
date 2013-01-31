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

#ifndef HYPERSPACE_TIMERHANDLER_H
#define HYPERSPACE_TIMERHANDLER_H

#include <boost/shared_ptr.hpp>
#include <boost/thread/condition.hpp>

#include "Common/Mutex.h"
#include "Common/Time.h"

#include "RangeServer.h"
#include "TimerInterface.h"

namespace Hypertable {

  /**
   */
  class TimerHandler : public TimerInterface {

  public:
    TimerHandler(Comm *comm, RangeServer *range_server);
    virtual void handle(Hypertable::EventPtr &event_ptr);
    virtual void schedule_maintenance();
    virtual void complete_maintenance_notify();
    virtual bool low_memory() { return m_app_queue_paused || m_low_physical_memory; }
    virtual void shutdown();

  private:
    Comm         *m_comm;
    RangeServer  *m_range_server;
    ApplicationQueuePtr m_app_queue;
    int64_t       m_query_cache_memory;
    int32_t       m_timer_interval;
    int32_t       m_current_interval;
    int64_t       m_last_low_memory_maintenance;
    bool          m_urgent_maintenance_scheduled;
    bool          m_app_queue_paused;
    bool          m_low_physical_memory;
    boost::xtime  m_last_maintenance;
    boost::xtime  m_pause_time;
    bool          m_maintenance_outstanding;

    void restart_app_queue();
    bool low_memory_mode();
  };
  typedef boost::intrusive_ptr<TimerHandler> TimerHandlerPtr;
}

#endif // HYPERSPACE_TIMERHANDLER_H

