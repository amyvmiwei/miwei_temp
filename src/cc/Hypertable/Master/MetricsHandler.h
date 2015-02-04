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
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
 * 02110-1301, USA.
 */

/// @file
/// Declarations for MetricsHandler.
/// This file contains declarations for MetricsHandler, a dispatch handler class
/// used to collect and publish %Master metrics.

#ifndef Master_MetricsHandler_h
#define Master_MetricsHandler_h

#include <AsyncComm/Comm.h>
#include <AsyncComm/DispatchHandler.h>

#include <Common/MetricsCollectorGanglia.h>
#include <Common/MetricsProcess.h>
#include <Common/Properties.h>
#include <Common/metrics>

#include <memory>
#include <mutex>

namespace Hypertable {

  /// @addtogroup Master
  /// @{

  /// Collects and publishes %Master metrics.
  /// This class acts as the metrics timer dispatch handler 
  class MetricsHandler : public DispatchHandler {
  public:

    /// Constructor.
    /// Initializes state, setting #m_collection_interval to the property
    /// <code>Hypertable.Monitoring.Interval</code> and starts a timer using
    /// this object as the handler.
    /// @param props %Properties object
    MetricsHandler(PropertiesPtr &props);

    /// Destructor.
    /// Cancels the timer.
    virtual ~MetricsHandler() {};

    /// Starts metrics collection.
    void start_collecting();

    /// Stops metrics collection.
    void stop_collecting();

    /// Collects and publishes metrics.
    /// This method computes and updates the <code>operations/s</code> and
    /// general process metrics and publishes them via #m_ganglia_collector.
    /// After metrics have been collected, the timer is re-registered for
    /// #m_collection_interval milliseconds in the future.
    /// @param event %Comm layer timer event
    virtual void handle(EventPtr &event);

    /// Increments operation count.
    /// Increments operation count which is used in computing operations/s.
    void operation_increment() {
      m_operations.current++;
    }

  private:

    /// Comm layer
    Comm *m_comm {};

    /// Ganglia metrics collector
    MetricsCollectorGangliaPtr m_ganglia_collector;

    /// General process metrics tracker
    MetricsProcess m_metrics_process;

    /// %Timestamp of last metrics collection
    int64_t m_last_timestamp;

    /// %Metrics collection interval
    int32_t m_collection_interval {};

    /// %Master operations
    interval_metric<int64_t> m_operations {};
  };

  /// Smart pointer to MetricsHandler
  typedef std::shared_ptr<MetricsHandler> MetricsHandlerPtr;

  /// @}
}

#endif // Master_MetricsHandler_h
