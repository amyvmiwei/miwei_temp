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
/// used to collect and publish %FsBroker metrics.

#ifndef FsBroker_Lib_MetricsHandler_h
#define FsBroker_Lib_MetricsHandler_h

#include <AsyncComm/Comm.h>
#include <AsyncComm/DispatchHandler.h>

#include <Common/MetricsCollectorGanglia.h>
#include <Common/MetricsProcess.h>
#include <Common/Properties.h>
#include <Common/metrics>

#include <memory>
#include <mutex>

namespace Hypertable {
namespace FsBroker {
namespace Lib {

  /// @addtogroup FsBrokerLib
  /// @{

  /// Collects and publishes %FsBroker metrics.
  /// This class acts as the timer dispatch handler for periodic metrics
  /// collection for FsBroker.
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
    /// @param type Type of broker (e.g. "local", "qfs", etc.)
    MetricsHandler(PropertiesPtr &props, const std::string &type);

    /// Destructor.
    /// Cancels the timer.
    virtual ~MetricsHandler() {};

    /// Starts metrics collection.
    void start_collecting();

    /// Stops metrics collection.
    void stop_collecting();

    /// Collects and publishes metrics.
    /// This method updates the <code>requests/s</code> and general process
    /// metrics and publishes them via #m_ganglia_collector.  After metrics have
    /// been collected, the timer is re-registered for #m_collection_interval
    /// milliseconds in the future.
    /// @param event %Comm layer timer event
    virtual void handle(EventPtr &event);

    /// Adds bytes read.
    /// Adds <code>count</code> to #m_bytes_read.
    /// @param count Count of bytes read
    void add_bytes_read(int64_t count) {
      std::lock_guard<std::mutex> lock(m_mutex);
      m_bytes_read += count;
    }

    /// Adds bytes written.
    /// Adds <code>count</code> to #m_bytes_written.
    /// @param count Count of bytes written
    void add_bytes_written(int64_t count) {
      std::lock_guard<std::mutex> lock(m_mutex);
      m_bytes_written += count;
    }

    /// Adds sync information.
    /// Adds <code>latency_nsec</code> to #m_sync_latency and increments
    /// #m_syncs.
    /// @param latency_nsec Latency of sync in nanoseconds
    void add_sync(int64_t latency_nsec) {
      std::lock_guard<std::mutex> lock(m_mutex);
      m_syncs++;
      m_sync_latency += (int)(latency_nsec/1000000LL);
    }

    /// Increments error count.
    /// Increments m_errors.
    void increment_error_count() {
      std::lock_guard<std::mutex> lock(m_mutex);
      m_errors++;
    }

  private:
    /// %Mutex for serializing access to members
    std::mutex m_mutex;

    /// Comm layer pointer
    Comm *m_comm;

    /// Ganglia metrics collector
    MetricsCollectorGangliaPtr m_ganglia_collector;

    /// General process metrics tracker
    MetricsProcess m_metrics_process;

    /// FsBroker type (e.g. "local", "qfs", etc.)
    std::string m_type;

    /// %Metrics collection interval
    int32_t m_collection_interval {};

    /// %Timestamp of last metrics collection
    int64_t m_last_timestamp;

    /// Bytes written since last metrics collection
    int64_t m_bytes_written {};

    /// Bytes read since last metrics collection
    int64_t m_bytes_read {};

    /// Cumulative sync latency since last metrics collection
    int32_t m_sync_latency {};

    /// Syncs since last metrics collection
    int32_t m_syncs {};

    /// Error count since last metrics collection
    int32_t m_errors {};

  };

  /// Smart pointer to MetricsHandler
  typedef std::shared_ptr<MetricsHandler> MetricsHandlerPtr;

  /// @}

}}}

#endif // FsBroker_Lib_MetricsHandler_h
