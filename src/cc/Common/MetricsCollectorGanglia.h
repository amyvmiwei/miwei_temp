/* -*- c++ -*-
 * Copyright (C) 2007-2014 Hypertable, Inc.
 *
 * This file is part of Hypertable.
 *
 * Hypertable is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; version 3
 * of the License.
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
/// Declarations for MetricsCollectorGanglia.
/// This file contains type declarations for MetricsCollectorGanglia, a simple class for
/// aggregating metrics and sending them to the Ganglia gmond process running on
/// localhost.

#ifndef Common_MetricsCollectorGanglia_h
#define Common_MetricsCollectorGanglia_h

#include "MetricsCollector.h"

#include <Common/Properties.h>

#include <memory>
#include <mutex>
#include <unordered_map>

namespace Hypertable {

  /// @addtogroup Common
  /// @{

  /// Ganglia metrics collector.
  class MetricsCollectorGanglia : public MetricsCollector {
  public:

    /// Constructor.
    /// Creates a datagram send socket and binds it to an arbitrary interface
    /// and ephemeral port.  Initializes #m_prefix to
    /// "ht." + <code>component</code> + ".".
    /// @param component Hypertable component ("fsbroker", "hyperspace, "master",
    /// "rangeserver", or "thriftbroker")
    /// @param props Properties object
    MetricsCollectorGanglia(const std::string &component, PropertiesPtr &props);

    /// Destructor.
    /// Closes datagram socket (#m_sd).
    ~MetricsCollectorGanglia();

    /// Updates string metric value.
    /// Inserts <code>value</code> into m_values_string map using a key that is
    /// formulated as m_prefix + <code>name</code>.
    /// @param name Relative name of metric
    /// @param value Metric value
    void update(const std::string &name, const std::string &value) override;

    /// Updates short integer metric value.
    /// Inserts <code>value</code> into m_values_int map using a key that is
    /// formulated as m_prefix + <code>name</code>.
    /// @param name Relative name of metric
    /// @param value Metric value
    void update(const std::string &name, int16_t value) override;

    /// Updates integer metric value.
    /// Inserts <code>value</code> into m_values_int map using a key that is
    /// formulated as m_prefix + <code>name</code>.
    /// @param name Relative name of metric
    /// @param value Metric value
    void update(const std::string &name, int32_t value) override;

    /// Updates float metric value.
    /// Inserts <code>value</code> into m_values_double map using a key that is
    /// formulated as m_prefix + <code>name</code>.
    /// @param name Relative name of metric
    /// @param value Metric value
    void update(const std::string &name, float value) override;

    /// Updates double metric value.
    /// Inserts <code>value</code> into m_values_double map using a key that is
    /// formulated as m_prefix + <code>name</code>.
    /// @param name Relative name of metric
    /// @param value Metric value
    void update(const std::string &name, double value) override;

    /// Publishes metric values to Ganglia hypertable extension.
    /// Constructs a JSON object containing the metrics key/value pairs
    /// constructed from the #m_values_string, #m_values_int, and
    /// #m_values_double maps.  The JSON string is sent to the the Ganglia
    /// hyperspace extension by sending it in the form of a datagram packet over
    /// #m_sd.
    void publish() override;

  private:

    /// Connects to Ganglia hypertable extension receive port.
    /// Connects #m_sd to "localhost", port #m_port.  Throws an exception on
    /// error, otherwise sets #m_connected to <i>true</i>.
    /// @throws Exception with code set to Error::COMM_CONNECT_ERROR
    void connect();

    /// %Mutex for serializing access to members
    std::mutex m_mutex;

    /// Metric name prefix ("ht." + component + ".")
    std::string m_prefix;

    /// Ganglia hypertable extension listen port
    uint16_t m_port {};

    /// Datagram send socket
    int m_sd {};

    /// Persistent string for holding JSON string
    std::string m_message;

    /// Map holding string metric values
    std::unordered_map<std::string, std::string> m_values_string;

    /// Map holding integer metric values
    std::unordered_map<std::string, int32_t> m_values_int;

    /// Map holding floating point metric values
    std::unordered_map<std::string, double> m_values_double;

    /// Flag indicating if socket is connected
    bool m_connected {};

    /// Flag indicating if publishing is disabled
    bool m_disabled {};
  };

  /// Smart pointer to MetricsCollectorGanglia
  typedef std::shared_ptr<MetricsCollectorGanglia> MetricsCollectorGangliaPtr;

  /// @}
}

#endif // Common_MetricsCollectorGanglia_h
