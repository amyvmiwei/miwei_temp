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
/// Declarations for Readdir response callback.
/// This file contains declarations for Readdir, a response callback class used
/// to deliver results of the <i>readdir</i> function call back to the client.

#ifndef FsBroker_Lib_Response_Callback_Readdir_h
#define FsBroker_Lib_Response_Callback_Readdir_h

#include <AsyncComm/CommBuf.h>
#include <AsyncComm/ResponseCallback.h>

#include <Common/Error.h>
#include <Common/Filesystem.h>

#include <vector>

namespace Hypertable {
namespace FsBroker {
namespace Lib {
namespace Response {
namespace Callback {

  /// @addtogroup FsBrokerLibResponseCallback
  /// @{

  /// Application handler for <i>readdir</i> function.
  class Readdir : public ResponseCallback {

  public:
    /// Constructor.
    /// Initializes parent class with <code>comm</code> and
    /// <code>event</code>.
    /// @param comm Pointer to comm layer
    /// @param event Comm layer event that instigated the request
    Readdir(Comm *comm, EventPtr &event) : ResponseCallback(comm, event) { }

    /// Sends response parameters back to client.
    /// @param listing Directory listing
    /// @return Error code returned by Comm::send_result
    int response(std::vector<Filesystem::Dirent> &listing);

  };

  /// @}

}}}}}


#endif // FsBroker_Lib_Response_Callback_Readdir_h
