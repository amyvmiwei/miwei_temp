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
 * Declarations for DispatchHandler.
 * This file contains type declarations for DispatchHandler, an abstract
 * base class for event handlers.
 */

#ifndef HYPERTABLE_DISPATCHHANDLER_H
#define HYPERTABLE_DISPATCHHANDLER_H

#include "Common/ReferenceCount.h"

#include "Event.h"

namespace Hypertable {

  /** @addtogroup AsyncComm
   *  @{
   */

  /** Abstract base class for event handlers registered with AsyncComm.
   * Event handlers are the mechanism by which an application is notified of
   * communication events.
   */
  class DispatchHandler : public ReferenceCount {
  public:

    /** Callback method.  When the Comm layer needs to deliver an event to the
     * application, this method is called to do so.  The set of event types
     * include, CONNECTION_ESTABLISHED, DISCONNECT, MESSAGE, ERROR, and TIMER.
     *
     * @param event_ptr smart pointer to Event object
     */
    virtual void handle(EventPtr &event_ptr) = 0;

    virtual ~DispatchHandler() { return; }
  };

  /// Smart pointer to DispatchHandler
  typedef intrusive_ptr<DispatchHandler> DispatchHandlerPtr;
  /** @}*/
} // namespace Hypertable

#endif // HYPERTABLE_DISPATCHHANDLER_H
