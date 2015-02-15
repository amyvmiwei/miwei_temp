/* -*- c++ -*-
 * Copyright (C) 2007-2015 Hypertable, Inc.
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
/// Declarations for DispatchHandlerOperationSystemStatus.
/// This file contains declarations for DispatchHandlerOperationSystemStatus, an
/// class for scattering status requests and gathering status responses for a
/// set of RangeServers.

#ifndef Hypertable_Master_DispatchHandlerOperationSystemStatus_h
#define Hypertable_Master_DispatchHandlerOperationSystemStatus_h

#include "DispatchHandlerOperation.h"

#include <AsyncComm/CommAddress.h>

#include <Common/Error.h>
#include <Common/SockAddrMap.h>
#include <Common/Status.h>
#include <Common/Timer.h>

#include <string>

namespace Hypertable {

  /// @addtogroup Master
  /// @{

  /// Carries out scatter/gather requests for RangeServer status.
  class DispatchHandlerOperationSystemStatus : public DispatchHandlerOperation {
  public:

    /// Holds result of a RangeServer status request.
    class Result {
    public:
      Result(const std::string &location, InetAddr addr)
        : location(location), addr(addr) {}
      std::string location;
      InetAddr addr;
      int32_t error {Error::NO_RESPONSE};
      std::string message;
      Status status;
    };

    /// Constructor.
    /// @param context %Master context
    /// @param timer Deadline timer
    DispatchHandlerOperationSystemStatus(ContextPtr &context, Timer &timer);

    /// Performs initialization.
    /// This method populates #m_index with pointers to the Result objects in
    /// <code>results</code>.  This means that while the the status operations
    /// are in progress, the caller should not modify the <code>results</code>
    /// vector in any way.  It also starts the deadline timer #m_timer.
    /// @param results Vector of result objects, one for each RangeServer.
    void initialize(std::vector<Result> &results);

    /// Issues a status request for the range server at <code>location</code>.
    /// @param location Proxy name of range server
    void start(const String &location) override;

    /// Handles a status request event.
    /// Locates the corresponding result object in #m_index and populates it with 
    void result_callback(const EventPtr &event) override;

  private:
    
    /// Deadline timer
    Timer m_timer;

    /// Result map
    SockAddrMap<Result *> m_index;
  };

  /// Smart pointer to DispatchHandlerOperationSystemStatus
  typedef std::shared_ptr<DispatchHandlerOperationSystemStatus> DispatchHandlerOperationSystemStatusPtr;

}

#endif // Hypertable_Master_DispatchHandlerOperationSystemStatus_h
