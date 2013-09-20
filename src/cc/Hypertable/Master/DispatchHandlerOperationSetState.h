/*
 * Copyright (C) 2007-2013 Hypertable, Inc.
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
 * Declarations for DispatchHandlerOperationSetState.
 * This file contains declarations for DispatchHandlerOperationSetState, a
 * dispatch handler class for gathering responses to a set of
 * RangeServer::set_state() requests.
 */

#ifndef HYPERTABLE_DISPATCHHANDLEROPERATIONSETSTATE_H
#define HYPERTABLE_DISPATCHHANDLEROPERATIONSETSTATE_H

#include "AsyncComm/CommAddress.h"

#include "Common/SockAddrMap.h"
#include "Common/Timer.h"

#include "DispatchHandlerOperation.h"
#include "RangeServerStatistics.h"

namespace Hypertable {

  /** @addtogroup Master
   *  @{
   */

  /** DispatchHandler class for gathering responses to a set of
   * RangeServer::set_state() requests.
   */
  class DispatchHandlerOperationSetState : public DispatchHandlerOperation {
  public:

    /** Constructor.
     * Initializes #m_timer with <code>Hypertable.Monitoring.Interval</code>
     * property.
     * @param context %Master context
     */
    DispatchHandlerOperationSetState(ContextPtr &context);
    
    /** Pre-request initialization.
     * This method calls SystemState::get() to populate #m_specs and
     * #m_generation, and starts #m_timer.
     */
    void initialize();

    /** Issues a RangeServer::set_state() request to a range server.
     * This method makes an asynchronous call to RangeServer::set_state()
     * @param location Proxy name of server
     */
    virtual void start(const String &location);

  private:

    /// %Request deadline timer
    Timer m_timer;

    /// Vector of system state variable specifications
    std::vector<SystemVariable::Spec> m_specs;

    /// Generation of system state variables
    uint64_t m_generation;
  };

  /// Smart pointer to DispatchHandlerOperationSetState
  typedef intrusive_ptr<DispatchHandlerOperationSetState> DispatchHandlerOperationSetStatePtr;

  /** @}*/
}

#endif // HYPERTABLE_DISPATCHHANDLEROPERATIONSETSTATE_H
