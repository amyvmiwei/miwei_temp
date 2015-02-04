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

#ifndef FsBroker_Lib_ConnectionHandlerFactory_h
#define FsBroker_Lib_ConnectionHandlerFactory_h

#include "Broker.h"
#include "ConnectionHandler.h"

#include <AsyncComm/ApplicationQueue.h>
#include <AsyncComm/ConnectionHandlerFactory.h>
#include <AsyncComm/Comm.h>
#include <AsyncComm/DispatchHandler.h>

namespace Hypertable {
namespace FsBroker {
namespace Lib {

  /// @addtogroup FsBrokerLib
  /// @{

  /**
   * This class serves as the connection handler factory.  An object of this
   * class is registered with the AsyncComm system by passing it as a
   * parameter to the Listen method.  When a connection request arrives, the
   * newInstance method of this class is called which creates an
   * FsBroker::connection_handler that will be used to service the
   * connection.
   */
  class ConnectionHandlerFactory
    : public Hypertable::ConnectionHandlerFactory {
  public:
    /**
     * Constructor.  Saves a copy of the pointers to the Comm,
     * ApplicationQueue, and Broker objects which are required in the
     * FsBroker::connection_handler constructor.
     *
     * @param comm pointer to the AsyncComm object
     * @param app_queue pointer to the application work queue
     * @param broker abstract pointer to the broker object
     */
    ConnectionHandlerFactory(Comm *comm, ApplicationQueuePtr &app_queue,
			     BrokerPtr &broker)
      : m_comm(comm), m_app_queue(app_queue), m_broker(broker) { }

    /**
     * Returns a newly constructed FsBroker::connection_handler object
     */
    virtual void get_instance(DispatchHandlerPtr &dhp) {
      dhp = make_shared<ConnectionHandler>(m_comm, m_app_queue, m_broker);
    }

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

#endif // FsBroker_Lib_ConnectionHandlerFactory_h
