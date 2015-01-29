/* -*- c++ -*-
 * Copyright (C) 2007-2014 Hypertable, Inc.
 *
 * This file is part of Hypertable.
 *
 * Hypertable is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; version 3 of the
 * License, or any later version.
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

#ifndef Hypertable_RangeServer_ConnectionHandler_h
#define Hypertable_RangeServer_ConnectionHandler_h

#include "RangeServer.h"

#include <AsyncComm/ApplicationQueue.h>
#include <AsyncComm/DispatchHandler.h>

namespace Hypertable {
class Comm;
namespace RangeServer {

  /// @addtogroup RangeServer
  /// @{

  class ConnectionHandler : public DispatchHandler {
  public:

    ConnectionHandler(Comm *comm, ApplicationQueuePtr &aq, Apps::RangeServer *rs)
      : m_comm(comm), m_app_queue(aq), m_range_server(rs) {
    }

    virtual void handle(EventPtr &event);

  private:
    Comm *m_comm {};
    ApplicationQueuePtr m_app_queue;
    Apps::RangeServer *m_range_server;
    bool m_shutdown {};
  };

  /// Smart pointer to ConnectionHandler
  typedef std::shared_ptr<ConnectionHandler> ConnectionHandlerPtr;

  /// @}

}}

#endif // Hypertable_RangeServer_ConnectionHandler_h

