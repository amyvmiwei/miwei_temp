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
 * Declarations for ResponseCallbackAttrGet.
 * This file contains declarations for ResponseCallbackAttrGet, a response
 * callback class for sending the result of a Master::attr_get() call
 * to the requesting client.
 */

#ifndef HYPERSPACE_RESPONSECALLBACKATTRGET_H
#define HYPERSPACE_RESPONSECALLBACKATTRGET_H

#include "Common/Error.h"

#include "AsyncComm/CommBuf.h"
#include "AsyncComm/ResponseCallback.h"

#include "Common/StaticBuffer.h"

namespace Hyperspace {

  /** @addtogroup hyperspaceResponse
   * @{
   */

  /** Sends back result of an <i>attr_get</i> request. */
  class ResponseCallbackAttrGet : public Hypertable::ResponseCallback {
  public:

    /** Constructor.
     * @param comm Comm instance
     * @param event %Comm event that originated the request
     */
    ResponseCallbackAttrGet(Hypertable::Comm *comm,
                            Hypertable::EventPtr &event)
      : Hypertable::ResponseCallback(comm, event) { }

    /** Sends back result of an <i>attr_get</i> request.
     * This method is called to send back the result of an <i>attr_get</i>
     * request.  A response message containing the result is encoded and sent
     * back to the requesting client with a call to Comm::send_response().  The
     * response message is encoded as follows:
     * <table>
     *   <tr><th>Encoding</th><th>Description</th></tr>
     *   <tr><td>i32</td><td>Error::OK</td></tr>
     *   <tr><td>i32</td><td>%Attribute count</td></tr>
     *   <tr><td>bytes32</td><td><b>foreach</b> attribute, fetched value</td></tr>
     * </table>
     * @param buffers Vector of attribute values
     * @return Error code returned from Comm::send_response()
     */
    int response(const std::vector<Hypertable::DynamicBufferPtr> &buffers);
  };

  /** @} */
}

#endif // HYPERSPACE_RESPONSECALLBACKATTRGET_H
