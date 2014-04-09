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

/// @file
/// Declarations for RequestHandlerTableMaintenanceDisable.
/// This file contains type declarations for
/// RequestHandlerTableMaintenanceDisable, an ApplicationHandler class for
/// carrying out a RangeServer::table_maintenance_disable() request.

#ifndef Hypertable_RangeServer_RequestHandler_TableMaintenanceDisable_h
#define Hypertable_RangeServer_RequestHandler_TableMaintenanceDisable_h

#include <AsyncComm/ApplicationHandler.h>
#include <AsyncComm/Comm.h>
#include <AsyncComm/Event.h>

namespace Hypertable {

  class RangeServer;

  /// @addtogroup RangeServer
  /// @{

  /// ApplicationHandler class for carrying out a
  /// RangeServer::table_maintenance_disable() request.
  class RequestHandlerTableMaintenanceDisable : public ApplicationHandler {
  public:

    /// Constructor.
    /// @param comm Pointer to comm layer
    /// @param rs Pointer to RangeServer
    /// @param event Smart pointer to event object initiating request
    RequestHandlerTableMaintenanceDisable(Comm *comm, RangeServer *rs,
                                          EventPtr &event)
      : ApplicationHandler(event), m_comm(comm), m_range_server(rs) { }

    /// Carries out RangeServer::table_maintenance_disable() request.
    /// This member function unmarshals the request parameters from #m_event and
    /// then calls the RangeServer::table_maintenance_disable() member function
    /// of #m_range_server.
    virtual void run();

  private:

    /// Pointer to comm layer
    Comm *m_comm;

    /// Pointer to RangeServer
    RangeServer *m_range_server;
  };

  /// @}
}

#endif // Hypertable_RangeServer_RequestHandler_TableMaintenanceDisable_h
