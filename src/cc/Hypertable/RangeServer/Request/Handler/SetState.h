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

/** @file
 * Declarations for RequestHandlerSetState.
 * This file contains the declaration for RequestHandlerSetState, a
 * class for de-marshalling request parameters and issuing a
 * RangeServer::set_state() request.
 */

#ifndef Hypertable_RangeServer_Request_Handler_SetState_h
#define Hypertable_RangeServer_Request_Handler_SetState_h

#include <AsyncComm/ApplicationHandler.h>
#include <AsyncComm/Comm.h>
#include <AsyncComm/Event.h>

namespace Hypertable {
namespace Apps { class RangeServer; }
namespace RangeServer {
namespace Request {
namespace Handler {

  /// @addtogroup RangeServerRequestHandler
  /// @{

  /// Decodes <i>set state</i> request and calls RangeServer::set_state().
  class SetState : public ApplicationHandler {
  public:

    /** Constructor.
     * @param comm Pointer to Comm object
     * @param rs Pointer to RangeServer object
     * @param event Request event object
     */
    SetState(Comm *comm, Apps::RangeServer *rs, EventPtr &event)
      : ApplicationHandler(event), m_comm(comm), m_range_server(rs) { }

    /** Decodes request and calls RangeServer::set_state().
     * This method decodes the request in the format:
     * <pre>
     * Generation number of state variables (i64)
     * Vector of state variable specs
     * </pre>
     */
    virtual void run();

  private:

    /// Pointer to Comm object
    Comm *m_comm;

    /// Pointer to RangeServer object
    Apps::RangeServer *m_range_server;
  };

  /// @}

}}}}

#endif // Hypertable_RangeServer_Request_Handler_SetState_h
