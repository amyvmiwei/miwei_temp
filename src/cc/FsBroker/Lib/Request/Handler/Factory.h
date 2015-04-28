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
/// Declarations for %FsBroker request handler Factory.
/// This file contains declarations for Factory, a factory class for creating
/// request handler for file system brokers.

#ifndef FsBroker_Lib_Request_Handler_Factory_h
#define FsBroker_Lib_Request_Handler_Factory_h

#include <FsBroker/Lib/Broker.h>

#include <AsyncComm/ApplicationHandler.h>
#include <AsyncComm/Comm.h>
#include <AsyncComm/Event.h>

namespace Hypertable {
namespace FsBroker {
namespace Lib {
namespace Request {
namespace Handler {

  /// @addtogroup FsBrokerLibRequestHandler
  /// @{

  /// %Factory class for constructing application handlers from request events.
  class Factory {

  public:

    /// Enumeration for request function codes
    enum Type {
      FUNCTION_OPEN = 0, ///< Open
      FUNCTION_CREATE,   ///< Create
      FUNCTION_CLOSE,    ///< Close
      FUNCTION_READ,     ///< Read
      FUNCTION_APPEND,   ///< Append
      FUNCTION_SEEK,     ///< Seek
      FUNCTION_REMOVE,   ///< Remove
      FUNCTION_SHUTDOWN, ///< Shutdown
      FUNCTION_LENGTH,   ///< Length
      FUNCTION_PREAD,    ///< Pread
      FUNCTION_MKDIRS,   ///< Mkdirs
      FUNCTION_STATUS,   ///< Status
      FUNCTION_FLUSH,    ///< Flush
      FUNCTION_RMDIR,    ///< Rmdir
      FUNCTION_READDIR,  ///< Readdir
      FUNCTION_EXISTS,   ///< Exists
      FUNCTION_RENAME,   ///< Rename
      FUNCTION_DEBUG,    ///< Debug
      FUNCTION_SYNC,     ///< Sync
      FUNCTION_MAX       ///< Maximum code marker
    };

    /// Constructs a handler class from a request event.
    /// @param comm Comm layer
    /// @param broker Pointer to file system broker object
    /// @param event Comm layer event instigating the request
    static ApplicationHandler *create(Comm *comm, Broker *broker,
				      EventPtr &event);
  };

  /// @}

}}}}}

#endif // FsBroker_Lib_Request_Handler_Factory_h
