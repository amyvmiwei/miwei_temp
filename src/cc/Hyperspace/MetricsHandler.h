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

/// @file
/// Declarations for MetricsHandler.
/// This file contains declarations for MetricsHandler, a dispatch handler class
/// used to collect and publish %Hyperspace metrics.

#ifndef Hyperspace_MetricsHandler_h
#define Hyperspace_MetricsHandler_h

#include <AsyncComm/DispatchHandler.h>

#include <Common/MetricsCollectorGanglia.h>
#include <Common/MetricsProcess.h>
#include <Common/Properties.h>
#include <Common/metrics>

#include <memory>
#include <mutex>

namespace Hyperspace {

  using namespace Hypertable;

  /// @addtogroup Hyperspace
  /// @{

  /// Collects and publishes %Hyperspace metrics.
  /// This class acts as the timer dispatch handler for periodic metrics
  /// collection for Hyperspace.
  class MetricsHandler : public DispatchHandler {
  public:

    /// Constructor.
    /// Initializes #m_collection_interval to the property
    /// <code>Hypertable.Monitoring.Interval</code> and allocates a Ganglia
    /// collector object, initializing it with "hyperspace" and
    /// <code>Hypertable.Metrics.Ganglia.Port</code>.  Lastly, calls
    /// Comm::set_timer() to register a timer for
    /// #m_collection_interval milliseconds in the future and passes
    /// <code>this</code> as the timer handler.
    /// @param props %Properties object
    MetricsHandler(PropertiesPtr &props);

    /// Destructor.
    /// Cancels the timer.
    virtual ~MetricsHandler();

    /// Collects and publishes metrics.
    /// This method updates the <code>requests/s</code> and general process
    /// metrics and publishes them via #m_ganglia_collector.  After metrics have
    /// been collected, the timer is re-registered for #m_collection_interval
    /// milliseconds in the future.
    /// @param event %Comm layer timer event
    virtual void handle(EventPtr &event);

    /// Increments request count.
    /// Increments #m_requests which is used in computing requests/s.
    void request_increment() {
      m_requests.current++;
    }

  private:

    /// Ganglia metrics collector
    MetricsCollectorGangliaPtr m_ganglia_collector;

    /// General process metrics tracker
    MetricsProcess m_metrics_process;

    /// %Timestamp of last metrics collection
    int64_t m_last_timestamp;

    /// %Metrics collection interval
    int32_t m_collection_interval {};

    /// %Hyperspace requests
    interval_metric<int64_t> m_requests {};
  };

  /// Smart pointer to MetricsHandler
  typedef std::shared_ptr<MetricsHandler> MetricsHandlerPtr;

  /// @}
}

#endif // Hyperspace_MetricsHandler_h
