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

/** @file
 * Declarations for RequestHandlerAttrDel.
 * This file contains declarations for RequestHandlerAttrDel, an application
 * handler class for unmarshalling request parameters and calling the
 * Master::attr_del() method.
 */

#ifndef HYPERSPACE_REQUESTHANDLERATTRDEL_H
#define HYPERSPACE_REQUESTHANDLERATTRDEL_H

#include "AsyncComm/ApplicationHandler.h"
#include "AsyncComm/Comm.h"
#include "AsyncComm/Event.h"

namespace Hyperspace {

  /** @addtogroup hyperspaceRequest
   * @{
   */

  class Master;

  /** Unmarshalls request parameters and calls Master::attr_del().
   */
  class RequestHandlerAttrDel : public ApplicationHandler {
  public:

    /** Constructor.
     * Initializes object by passing <code>event</code> into parent constructor,
     * setting #m_comm to <code>comm</code>, #m_master to <code>master</code>,
     * and #m_session_id to <code>session_id</code>.
     * @param comm %Comm instance
     * @param master %Master pointer
     * @param session_id %Session ID
     * @param event %Event object generating request
     */
     RequestHandlerAttrDel(Comm *comm, Master *master, uint64_t session_id,
                           EventPtr &event)
       : ApplicationHandler(event), m_comm(comm), m_master(master),
         m_session_id(session_id) { }

    /** Unmarshalls request parameters and calls Master::attr_del().
     * This method unmarshalls request parameters from the originating Event
     * object and passes them into a call to Master::attr_del().  See
     * Protocol::create_attr_del_request() for the request parameter
     * encoding format.
     */
    virtual void run();

  private:

    /// Comm instance
    Comm *m_comm;

    /// %Hyperspace master
    Master *m_master;

    /// %Session ID
    uint64_t m_session_id;
  };

  /** @} */
}

#endif // HYPERSPACE_REQUESTHANDLERATTRDEL_H
