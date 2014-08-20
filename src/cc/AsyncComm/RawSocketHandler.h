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
/// Declarations for RawSocketHandler.
/// This file contains type declarations for RawSocketHandler, an abstract
/// base class from which are derived application handlers for responding to
/// raw socket events.

#ifndef AsyncComm_RawSocketHandler_h
#define AsyncComm_RawSocketHandler_h

#include <string>

namespace Hypertable {

  /// @addtogroup AsyncComm
  /// @{

  /// Abstract base class for application raw socket handlers registered with
  /// AsyncComm.  Allows applications to register a socket with the AsyncComm
  /// polling mechanism and perform raw event handling on the socket.
  class RawSocketHandler {
  public:

    /// Destructor
    virtual ~RawSocketHandler() { return; }

    /// Handle socket event.
    /// @param sd Socket descriptor
    /// @param events Bitmask of polling events
    /// @see PollEvent::Flags
    virtual bool handle(int sd, int events) = 0;

    /// Deregister handler for a given socket.
    /// This method is called by the communication layer after the socket has
    /// been removed from the polling mechanism.
    /// @param sd Socket descripter that was deregistered
    virtual void deregister(int sd) = 0;

    /// Returns desired polling interest for a socket.
    /// This method returns the current polling interest for socket
    /// <code>sd</code>.  After calling handle(), the communication layer will
    /// call this method to obtain the polling interest for <code>sd</code> and
    /// will make the appropriate adjustments with the underlying polling
    /// mechanism.
    /// @param sd Socket descriptor 
    /// @return Bitmask of desired polling interest for <code>sd</code>.
    /// @see PollEvent::Flags
    virtual int poll_interest(int sd) = 0;

  };

  /// @}

} // namespace Hypertable

#endif // AsyncComm_RawSocketHandler_h
