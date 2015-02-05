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
/// Declarations for Status response callback.
/// This file contains declarations for Status, a response callback class used
/// to deliver results of the <i>status</i> function call back to the client.

#ifndef Hypertable_RangeServer_Response_Callback_Status_h
#define Hypertable_RangeServer_Response_Callback_Status_h

#include <Hypertable/Lib/StatsRangeServer.h>

#include <AsyncComm/ResponseCallback.h>

#include <Common/StaticBuffer.h>
#include <Common/Status.h>

namespace Hypertable {
namespace RangeServer {
namespace Response {
namespace Callback {

  /// @addtogroup RangeServerResponseCallback
  /// @{

  /// Response callback for <i>status</i> function.
  class Status : public ResponseCallback {
  public:

    /// Constructor.
    /// Initializes base class constructor.
    /// @param comm Pointer to comm layer
    /// @param event Event object that initiated the request
    Status(Comm *comm, EventPtr &event)
      : ResponseCallback(comm, event) { }

    /// Sends response parameters back to client.
    /// @param status Status information
    /// @return Error code returned by Comm::send_result
    int response(Hypertable::Status &status);

  };

  /// @}

}}}}

#endif // Hypertable_RangeServer_Response_Callback_Status_h
