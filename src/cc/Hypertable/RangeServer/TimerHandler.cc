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
#include "Common/Compat.h"
#include "Common/Config.h"
#include "Common/Error.h"
#include "Common/InetAddr.h"
#include "Common/StringExt.h"
#include "Common/System.h"
#include "Common/Time.h"

#include <algorithm>
#include <sstream>

#include "Common/Time.h"

#include "RequestHandlerDoMaintenance.h"
#include "TimerHandler.h"

#include "Hypertable/Lib/KeySpec.h"

using namespace Hypertable;
using namespace Hypertable::Config;

/**
 *
 */
TimerHandler::TimerHandler(Comm *comm, RangeServer *range_server)
  : m_comm(comm), m_range_server(range_server),
    m_last_low_memory_maintenance(TIMESTAMP_NULL),
    m_urgent_maintenance_scheduled(false), m_app_queue_paused(false),
    m_low_physical_memory(false), m_maintenance_outstanding(false) {
  int error;
  int32_t maintenance_interval;

  m_query_cache_memory = get_i64("Hypertable.RangeServer.QueryCache.MaxMemory");
  m_timer_interval = get_i32("Hypertable.RangeServer.Timer.Interval");
  maintenance_interval = get_i32("Hypertable.RangeServer.Maintenance.Interval");

  if (m_timer_interval > (maintenance_interval+10)) {
    m_timer_interval = maintenance_interval + 10;
    HT_INFOF("Reducing timer interval to %d to support maintenance interval %d",
             m_timer_interval, maintenance_interval);
  }

  m_current_interval = m_timer_interval;

  boost::xtime_get(&m_last_maintenance, TIME_UTC_);

  m_app_queue = m_range_server->get_application_queue();

  range_server->register_timer(this);

  if ((error = m_comm->set_timer(0, this)) != Error::OK)
    HT_FATALF("Problem setting timer - %s", Error::get_text(error));

  return;
}


void TimerHandler::schedule_maintenance() {
  ScopedLock lock(m_mutex);

  if (!m_urgent_maintenance_scheduled && !m_maintenance_outstanding) {
    boost::xtime now;
    boost::xtime_get(&now, TIME_UTC_);
    uint64_t elapsed = xtime_diff_millis(m_last_maintenance, now);
    int error;
    uint32_t millis = (elapsed < 1000) ? 1000 - elapsed : 0;
    HT_INFOF("Scheduling urgent maintenance for %u millis in the future", millis);
    if ((error = m_comm->set_timer(millis, this)) != Error::OK)
      HT_FATALF("Problem setting timer - %s", Error::get_text(error));
    m_urgent_maintenance_scheduled = true;
  }
  return;
}


void TimerHandler::complete_maintenance_notify() {
  ScopedLock lock(m_mutex);
  m_maintenance_outstanding = false;
  boost::xtime_get(&m_last_maintenance, TIME_UTC_);
  if (m_app_queue_paused) {
    if (!low_memory_mode()) {
      restart_app_queue();
    }
  }
}


void TimerHandler::shutdown() {
  ScopedLock lock(m_mutex);
  TimerInterface::shutdown();
  m_comm->cancel_timer(this);
  // gracfully complete shutdown
  int error;
  if ((error = m_comm->set_timer(0, this)) != Error::OK)
    HT_FATALF("Problem setting timer - %s", Error::get_text(error));
}


/**
 *
 */
void TimerHandler::handle(Hypertable::EventPtr &event_ptr) {
  ScopedLock lock(m_mutex);
  int error;
  bool do_maintenance = !m_maintenance_outstanding;

  if (m_shutdown) {
    HT_INFO("TimerHandler shutting down.");
    m_shutdown_complete = true;
    m_shutdown_cond.notify_all();
    return;
  }

  if (m_range_server->replay_finished() && low_memory_mode()) {
    if (!m_app_queue_paused) {
      HT_INFO("Pausing application queue due to low memory condition");
      m_app_queue_paused = true;
      m_app_queue->stop();
      m_current_interval = 500;
      boost::xtime_get(&m_pause_time, TIME_UTC_);
    }
  }
  else if (m_app_queue_paused) {
    restart_app_queue();
  }

  if (low_memory()) {
    int64_t now = get_ts64();
    // balance maintenance scheduling up on recent maintenance
    // and maintenance workload
    if (m_last_low_memory_maintenance != TIMESTAMP_NULL) {
      if ((now-m_last_low_memory_maintenance) < (m_timer_interval*1000000LL) &&
	  Global::maintenance_queue->full())
        do_maintenance = false;
    }
    if (do_maintenance)
      m_last_low_memory_maintenance = now;
  }

  try {

    if (event_ptr->type == Hypertable::Event::TIMER) {

      if (do_maintenance) {
        m_app_queue->add( new RequestHandlerDoMaintenance(m_comm, m_range_server) );
        m_maintenance_outstanding = true;
      }

      if (m_urgent_maintenance_scheduled)
        m_urgent_maintenance_scheduled = false;
      else {
        //HT_INFOF("About to reset timer to %u millis in the future", m_current_interval);
        if ((error = m_comm->set_timer(m_current_interval, this)) != Error::OK)
          HT_FATALF("Problem setting timer - %s", Error::get_text(error));
      }
    }
    else
      HT_ERRORF("Unexpected event - %s", event_ptr->to_str().c_str());
  }
  catch (Hypertable::Exception &e) {
    HT_ERROR_OUT << e << HT_END;
  }
}

void TimerHandler::restart_app_queue() {
  HT_ASSERT(m_app_queue_paused);
  HT_INFO("Restarting application queue");
  m_app_queue->start();
  m_app_queue_paused = false;
  m_last_low_memory_maintenance = TIMESTAMP_NULL;
  m_current_interval = m_timer_interval;

  std::stringstream ss;
  boost::xtime now;
  boost::xtime_get(&now, TIME_UTC_);
  ss << now.sec << "\tapp-queue-pause\t" << xtime_diff_millis(m_pause_time, now);
  m_range_server->write_profile_data(ss.str());
}

bool TimerHandler::low_memory_mode() {
  int64_t memory_used = Global::memory_tracker->balance();

  // ensure unused physical memory if it makes sense
  if (Global::memory_limit_ensure_unused_current &&
      memory_used - m_query_cache_memory > Global::memory_limit_ensure_unused_current) {

    // adjust current limit according to the actual memory situation
    int64_t free_memory = (int64_t)(System::mem_stat().free * Property::MiB);
    if (Global::memory_limit_ensure_unused_current < Global::memory_limit_ensure_unused)
      Global::memory_limit_ensure_unused_current = std::min(free_memory, Global::memory_limit_ensure_unused);

    // low physical memory reached?
    m_low_physical_memory = free_memory < Global::memory_limit_ensure_unused_current;
    if (m_low_physical_memory)
       HT_INFOF("Low physical memory (free %.2fMB, limit %.2fMB)", free_memory / (double)Property::MiB,
                 Global::memory_limit_ensure_unused_current / (double)Property::MiB);
  }
  else {
    // don't care
    m_low_physical_memory = false;
  }
  return memory_used > Global::memory_limit;
}
