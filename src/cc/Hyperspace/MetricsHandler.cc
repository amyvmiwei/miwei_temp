/*
 * Copyright (C) 2007-2014 Hypertable, Inc.
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

#include <Common/Compat.h>

#include "MetricsHandler.h"

#include <AsyncComm/Comm.h>

#include <Common/Error.h>
#include <Common/Logger.h>

#include <mutex>

using namespace Hyperspace;
using namespace Hypertable;
using namespace std;

namespace {
  mutex g_mutex {};
  Comm *g_comm {};
}


MetricsHandler::MetricsHandler(PropertiesPtr &props)
  : m_previous_stats(StatsSystem::PROCINFO|StatsSystem::PROC) {
  m_ganglia_metrics = std::make_shared<GangliaMetrics>(props->get_i16("Hypertable.Metrics.Ganglia.Port"));
  m_collection_interval = props->get_i32("Hypertable.Monitoring.Interval");
  m_previous_stats.refresh();
  m_previous_timestamp = Hypertable::get_ts64();

  {
    lock_guard<mutex> lock(g_mutex);
    int error;
    g_comm = Comm::instance();
    if ((error = g_comm->set_timer(m_collection_interval, this)) != Error::OK)
      HT_FATALF("Problem setting timer - %s", Error::get_text(error));
  }

}

MetricsHandler::~MetricsHandler() {
  lock_guard<mutex> lock(g_mutex);
  g_comm->cancel_timer(this);
  g_comm = 0;
}


void MetricsHandler::handle(Hypertable::EventPtr &event) {
  lock_guard<mutex> lock(g_mutex);
  int error;

  if (g_comm == 0)
    return;

  if (event->type == Hypertable::Event::TIMER) {
    StatsSystem system_stats;
    int64_t timestamp;

    system_stats.refresh();
    timestamp = Hypertable::get_ts64();

    int64_t elapsed_millis = (timestamp-m_previous_timestamp) / 1000000LL;

    {
      lock_guard<mutex> lock(m_mutex);
      m_ganglia_metrics->update("hypertable.hyperspace.requests",
                                m_request_count.rate(elapsed_millis/1000));
      m_request_count.reset();
    }

    // CPU user time
    int32_t pct = ((system_stats.proc_stat.cpu_user - m_previous_stats.proc_stat.cpu_user) * 100) / elapsed_millis;
    m_ganglia_metrics->update("hypertable.hyperspace.cpu.user", pct);

    // CPU sys time
    pct = ((system_stats.proc_stat.cpu_sys - m_previous_stats.proc_stat.cpu_sys) * 100) / elapsed_millis;
    m_ganglia_metrics->update("hypertable.hyperspace.cpu.sys", pct);

    m_ganglia_metrics->update("hypertable.hyperspace.memory.virtual",
                              (float)system_stats.proc_stat.vm_size / 1024.0);

    m_ganglia_metrics->update("hypertable.hyperspace.memory.resident",
                              (float)system_stats.proc_stat.vm_resident / 1024.0);

    m_ganglia_metrics->update("hypertable.hyperspace.memory.majorFaults",
                              (int32_t)system_stats.proc_stat.major_faults);

    m_ganglia_metrics->update("hypertable.hyperspace.memory.heap",
                              (float)system_stats.proc_stat.heap_size / 1000000000.0);

    m_ganglia_metrics->update("hypertable.hyperspace.memory.heapSlack",
                              (float)system_stats.proc_stat.heap_slack / 1000000000.0);


    if (!m_ganglia_metrics->send())
      HT_INFOF("Problem sending Ganglia metrics - %s", m_ganglia_metrics->get_error());

    m_previous_stats = system_stats;
    m_previous_timestamp = timestamp;

    if ((error = g_comm->set_timer(m_collection_interval, this)) != Error::OK)
      HT_FATALF("Problem setting timer - %s", Error::get_text(error));

  }
  else
    HT_FATALF("Unrecognized event - %s", event->to_str().c_str());

}
