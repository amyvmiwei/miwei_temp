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

#include <Common/GangliaMetrics.h>
#include <Common/Properties.h>
#include <Common/StatsSystem.h>
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
      m_request_count.current++;
    }

  private:
    std::mutex m_mutex;
    GangliaMetricsPtr m_ganglia_metrics;
    StatsSystem m_previous_stats;
    int64_t m_previous_timestamp;
    int32_t m_collection_interval {};
    rate_metric<int64_t> m_request_count {};
  };

  typedef std::shared_ptr<MetricsHandler> MetricsHandlerPtr;

}

#endif // Hyperspace_MetricsHandler_h
