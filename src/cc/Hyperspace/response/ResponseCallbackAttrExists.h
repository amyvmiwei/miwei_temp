/* -*- c++ -*-
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
 * Declarations for ResponseCallbackAttrExists.
 * This file contains declarations for ResponseCallbackAttrExists, a response
 * callback class for sending the result of a Master::attr_exists() call
 * to the requesting client.
 */

#ifndef HYPERSPACE_RESPONSECALLBACKATTREXISTS_H
#define HYPERSPACE_RESPONSECALLBACKATTREXISTS_H

#include "AsyncComm/Comm.h"
#include "AsyncComm/ResponseCallback.h"

namespace Hyperspace {

  /** @addtogroup hyperspaceResponse
   * @{
   */

  /** Sends back result of an <i>attr_exists</i> request. */
  class ResponseCallbackAttrExists : public Hypertable::ResponseCallback {
  public:

    /** Constructor.
     * @param comm Comm instance
     * @param event Comm event that originated the request
     */
    ResponseCallbackAttrExists(Hypertable::Comm *comm,
                               Hypertable::EventPtr &event)
      : Hypertable::ResponseCallback(comm, event) { }
    
    /** Sends back result of an <i>attr_exists</i> request.
     * This method is called to send back the result of an <i>attr_exists</i>
     * request.  A response message containing the result is encoded and sent
     * back to the requesting client with a call to Comm::send_response().  The
     * response message is encoded as follows:
     * <table>
     *   <tr><th>Encoding</th><th>Description</th></tr>
     *   <tr><td>i32</td><td>Error::OK</td></tr>
     *   <tr><td>bool</td><td><code>exists</code></td></tr>
     * </table>
     * @param exists <i>true</i> if requested attribute exists, <i>false</i>
     * otherwise
     * @return Error code returned from Comm::send_response()
     */
    int response(bool exists);
  };

  /** @} */
}

#endif // HYPERSPACE_RESPONSECALLBACKATTREXISTS_H
