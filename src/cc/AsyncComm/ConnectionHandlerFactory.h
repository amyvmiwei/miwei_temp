/*
 * Copyright (C) 2007-2013 Hypertable, Inc.
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

/** @file
 * Declarations for ConnectionHandlerFactory.
 * This file contains type declarations for ConnectionHandlerFactory, a class
 * for creating default application dispatch handlers.
 */

#ifndef AsyncComm_ConnectionHandlerFactory_h
#define AsyncComm_ConnectionHandlerFactory_h

#include "DispatchHandler.h"

namespace Hypertable {

  /** @addtogroup AsyncComm
   *  @{
   */

  /** Abstract class for creating default application dispatch handlers.
   */
  class ConnectionHandlerFactory {
  public:
    /** Destructor */
    virtual ~ConnectionHandlerFactory() { }
    
    /** Creates a connection dispatch handler.
     * @param dhp Reference to created dispatch handler
     */
    virtual void get_instance(DispatchHandlerPtr &dhp) = 0;
  };

  /// Smart pointer to ConnectionHandlerFactory
  typedef std::shared_ptr<ConnectionHandlerFactory> ConnectionHandlerFactoryPtr;
  /** @}*/
}

#endif // AsyncComm_ConnectionHandlerFactory_h

