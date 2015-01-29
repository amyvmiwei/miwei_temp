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
 * Declarations for ConnectionHandler.
 * This file contains declarations for ConnectionHandler, a class for handling
 * incoming Master requests.
 */

#ifndef HYPERTABLE_CONNECTIONHANDLER_H
#define HYPERTABLE_CONNECTIONHANDLER_H

#include "AsyncComm/DispatchHandler.h"

#include "Operation.h"
#include "Context.h"

namespace Hypertable {

  /** @addtogroup Master
   *  @{
   */

  /** Handles incoming Master requests.
   * An object of this class is installed as the dispatch handler for the listen
   * socket on which the Master receives client connections and requests. To
   * handle transparent master failover while requests are being carried out,
   * most requests happen in two phases:
   *
   *  1. An operation is created to carry out the request and its ID is sent
   *     back to client
   *  2. %Client issues MasterProtocol::COMMAND_FETCH_RESULT request to
   *     fetch result of operation
   * 
   * After the operation has been created and persisted to the MML and the ID
   * has been sent back to the client, the Master can fail and when the new
   * Master comes up, it will complete the operation and send the results back
   * to the client.  This class also installs an interval timer and installs
   * itself as the handler.  When handling TIMER events, it checks to see if any
   * of the following actions are necessary, and if so, it will create
   * operations to carry them out.
   *
   *   - Statistics gathering
   *   - Garbage collection
   *   - Balancing
   */
  class ConnectionHandler : public DispatchHandler {
  public:
    /** Constructor.
     * This initializes the connection handler and installs the
     * interval timer.
     * @param context %Master context object
     */
    ConnectionHandler(ContextPtr &context);

    void start_timer();

    /** Responds to Master request events.
     * @param event AsyncComm event corresponding to Master request
     */
    virtual void handle(EventPtr &event);

  private:

    /** Sends operation ID back to client.
     * @param event AsyncComm event corresponding to Master request
     * @param operation Operation whose ID is to be sent back
     */
    int32_t send_id_response(EventPtr &event, OperationPtr &operation);

    /** Sends error response message back to client.
     * This method sends an error respons message back to the client formatted as
     * 32-bit error code by vstring error message.
     * @param event AsyncComm event corresponding to Master request
     * @param error Error code
     * @param msg Error message
     * @return Error::OK if response send back successfully, otherwise error code
     * returned by Comm::send_response
     */
    int32_t send_error_response(EventPtr &event, int32_t error, const String &msg);

    /** Sends OK response message back to client.
     * @param event AsyncComm event corresponding to Master request
     * @return Error::OK if response send back successfully, otherwise error code
     * returned by Comm::send_response
     */
    int32_t send_ok_response(EventPtr &event);

    /** Maybe dumps OperationProcessor statistics.
     * This method check for the existance of the file
     * <code>$HT_INSTALL_DIR/run/debug-op</code> and if it exists, it will
     * remove it and then call OperationProcessor::state_description() and
     * will write the state description message to the file
     * <code>$HT_INSTALL_DIR/run/op.output</code>.
     */
    void maybe_dump_op_statistics();

    /// Pointer to %Master context 
    ContextPtr m_context;

    /// Flag indicating that shutdown is in progress
    bool m_shutdown;
  };

  /** @}*/
}

#endif // HYPERTABLE_CONNECTIONHANDLER_H

