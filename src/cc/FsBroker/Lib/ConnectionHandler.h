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
/// Declarations for ConnectionHandler.
/// This file contains declarations for ConnectionHandler, a class that handles
/// incoming client requests.

#ifndef FsBroker_Lib_ConnectionHandler_h
#define FsBroker_Lib_ConnectionHandler_h

#include "Broker.h"

#include <AsyncComm/ApplicationQueue.h>
#include <AsyncComm/DispatchHandler.h>

namespace Hypertable {
namespace FsBroker {
namespace Lib {

  /// @addtogroup FsBrokerLib
  /// @{

  /// Connection handler for file system brokers.
  class ConnectionHandler : public DispatchHandler {

  public:
    /// Constructor.
    /// Initializes #m_comm with <code>comm</code>, #m_app_queue with
    /// <code>app_queue</code>, and #m_broker with <code>broker</code>.
    /// @param comm Pointer to comm layer
    /// @param app_queue Application queue
    /// @param broker Pointer to file system broker object
    ConnectionHandler(Comm *comm, ApplicationQueuePtr &app_queue,
		      BrokerPtr &broker)
      : m_comm(comm), m_app_queue(app_queue), m_broker(broker) { }

    /// Handles incoming events from client connections.
    /// For Event::MESSAGE events, this method inspects the command code and
    /// creates an appropriate request handler and adds it to the application
    /// queue.
    /// @param event Comm layer event
    virtual void handle(EventPtr &event);

  private:
    /// Pointer to comm layer
    Comm *m_comm;

    /// Application queue
    ApplicationQueuePtr m_app_queue;

    /// Pointer to file system broker object
    BrokerPtr m_broker;
  };

  /// @}

}}}

#endif // FsBroker_Lib_ConnectionHandler_h

