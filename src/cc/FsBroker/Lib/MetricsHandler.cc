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

/// @file
/// Declarations for MetricsHandler.
/// This file contains declarations for MetricsHandler, a dispatch handler class
/// used to collect and publish %FsBroker metrics.

#include <Common/Compat.h>

#include "MetricsHandler.h"

#include <AsyncComm/Comm.h>

#include <Common/Error.h>
#include <Common/Logger.h>
#include <Common/Properties.h>

#include <mutex>

using namespace Hypertable;
using namespace FsBroker;
using namespace std;

namespace {
  mutex g_mutex {};
  Comm *g_comm {};
}


MetricsHandler::MetricsHandler(PropertiesPtr &props, const std::string &type)
  : m_type(type) {
  int16_t port = props->get_i16("Hypertable.Metrics.Ganglia.Port");
  m_ganglia_collector = std::make_shared<MetricsCollectorGanglia>("fsbroker", port);
  m_collection_interval = props->get_i32("Hypertable.Monitoring.Interval");
  m_last_timestamp = Hypertable::get_ts64();
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

    int64_t now = Hypertable::get_ts64();
    m_metrics_process.collect(now, m_ganglia_collector.get());
    int64_t elapsed_millis = (now - m_last_timestamp) / 1000000LL;
    int64_t elapsed_seconds = elapsed_millis / 1000;
    m_ganglia_collector->update("type", m_type);

    {
      lock_guard<mutex> lock(m_mutex);

      m_ganglia_collector->update("errors", m_errors);

      int32_t avgSyncLatency = (m_syncs > 0) ? m_sync_latency/m_syncs : 0;
      m_ganglia_collector->update("syncLatency", avgSyncLatency);

      if (elapsed_millis > 0) {
        double sps = (double)m_syncs / (double)elapsed_seconds;
        m_ganglia_collector->update("syncs", sps);
        int64_t mbps = (m_bytes_read / 1000000) / elapsed_seconds;
        m_ganglia_collector->update("readThroughput", (int)mbps);
        mbps = (m_bytes_written / 1000000) / elapsed_seconds;
        m_ganglia_collector->update("writeThroughput", (int)mbps);
      }

      m_last_timestamp = now;
      m_errors = 0;
      m_syncs = 0;
      m_sync_latency = 0;
      m_bytes_read = 0;
      m_bytes_written = 0;
    }

    try {
      m_ganglia_collector->publish();
    }
    catch (Exception &e) {
      HT_INFOF("Problem publishing Ganglia metrics - %s", e.what());
    }

    if ((error = g_comm->set_timer(m_collection_interval, this)) != Error::OK)
      HT_FATALF("Problem setting timer - %s", Error::get_text(error));

  }
  else
    HT_FATALF("Unrecognized event - %s", event->to_str().c_str());

}
