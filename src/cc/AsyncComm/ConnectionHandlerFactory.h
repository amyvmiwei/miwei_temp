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
 * for creating default event handlers.
 */

#ifndef HYPERTABLE_CONNECTIONHANDLERFACTORY_H
#define HYPERTABLE_CONNECTIONHANDLERFACTORY_H

#include "Common/ReferenceCount.h"

#include "DispatchHandler.h"

namespace Hypertable {


  /** @addtogroup AsyncComm
   *  @{
   */

  /** Abstract class for creating default event handlers.
   */
  class ConnectionHandlerFactory : public ReferenceCount {
  public:
    virtual ~ConnectionHandlerFactory() { }
    virtual void get_instance(DispatchHandlerPtr &dhp) = 0;
  };

  /// Smart pointer to ConnectionHandlerFactory
  typedef intrusive_ptr<ConnectionHandlerFactory> ConnectionHandlerFactoryPtr;
  /** @}*/
}

#endif // HYPERTABLE_CONNECTIONHANDLERFACTORY_H

