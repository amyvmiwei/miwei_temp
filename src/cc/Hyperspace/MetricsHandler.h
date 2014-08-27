/* -*- c++ -*-
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

#ifndef Hyperspace_MetricsHandler_h
#define Hyperspace_MetricsHandler_h

#include <AsyncComm/DispatchHandler.h>

#include <Common/MetricsCollectorGanglia.h>
#include <Common/MetricsProcess.h>
#include <Common/Properties.h>
#include <Common/metrics.h>

#include <memory>
#include <mutex>

namespace Hyperspace {

  using namespace Hypertable;

  class MetricsHandler : public DispatchHandler {
  public:

    MetricsHandler(PropertiesPtr &props);

    virtual ~MetricsHandler();

    virtual void handle(EventPtr &event);

    void request_count_increment() {
      std::lock_guard<std::mutex> lock(m_mutex);
      m_requests.current++;
    }

  private:
    std::mutex m_mutex;
    MetricsCollectorGangliaPtr m_ganglia_collector;
    MetricsProcess m_metrics_process;
    int64_t m_last_timestamp;
    int32_t m_collection_interval {};
    interval_metric<int64_t> m_requests {};
  };

  typedef std::shared_ptr<MetricsHandler> MetricsHandlerPtr;

}

#endif // Hyperspace_MetricsHandler_h
