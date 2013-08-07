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
 * Declarations for OperationSetState.
 * This file contains declarations for OperationSetState, an Operation class
 * for setting system state variables.
 */

#ifndef HYPERTABLE_OPERATIONSETVARIABLES_H
#define HYPERTABLE_OPERATIONSETVARIABLES_H

#include "Common/StringExt.h"

#include "Hypertable/Lib/SystemVariable.h"

#include "Operation.h"

namespace Hypertable {

  /** @addtogroup Master
   *  @{
   */

  /** Carries out a set variables operation */
  class OperationSetState : public Operation {
  public:

    /** Constructor.
     * This constructor is used to create a SetState operation that is the
     * result of an automated condition change.  It initializes the
     * #m_client_originated to <i>false</i>.
     * @param context %Master context
     */
    OperationSetState(ContextPtr &context);

    /** Constructor.
     * This constructor is used to create a SetState operation from a Metalog
     * (MML) entry.
     * @param context %Master context
     * @param header_ MML entity header
     */
    OperationSetState(ContextPtr &context, const MetaLog::EntityHeader &header_);

    /** Constructor.
     * This constructor is used to create a SetState operation from a client
     * request event.  It initializes the #m_client_originated to <i>true</i>.
     * @param context %Master context
     * @param event Reference to event object
     */
    OperationSetState(ContextPtr &context, EventPtr &event);

    /** Destructor. */
    virtual ~OperationSetState() { }

    /** Executes "set state" operation.
     */
    virtual void execute();
    
    /** Returns operation name (OperationSetState)
     * @return Operation name
     */
    virtual const String name();

    
    virtual const String label();

    /** Displays textual representation of object state.
     * This method prints each variable and value in #m_specs
     * vector.
     */
    virtual void display_state(std::ostream &os);

    /** Returns length of encoded state. */
    virtual size_t encoded_state_length() const;

    /** Encodes operation state.
     * This method encodes the operation state formatted as follows:
     * <pre>
     *   Format version number
     *   System state generation number
     *   Number of variable specs to follow
     *   foreach variable spec {
     *     variable code
     *     variable value
     *   }
     *   Client originated flag
     * </pre>
     * @param bufp Address of destination buffer pointer (advanced by call)
     */
    virtual void encode_state(uint8_t **bufp) const;

    /** Decodes operation state.
     * See encode_state() for format description.
     * @param bufp Address of destination buffer pointer (advanced by call)
     * @param remainp Address of integer holding amount of remaining buffer
     */
    virtual void decode_state(const uint8_t **bufp, size_t *remainp);

    /** Decodes client request.
     * This method decodes a client request of the following format:
     * <pre>
     *   Number of variable specs to follow
     *   foreach variable spec {
     *     variable code
     *     variable value
     *   }
     * </pre>
     * @param bufp Address of destination buffer pointer (advanced by call)
     * @param remainp Address of integer holding amount of remaining buffer
     */
    virtual void decode_request(const uint8_t **bufp, size_t *remainp);

  private:

    /** Initializes operation dependencies.
     * This method initializes the operation dependencies which include:
     *   - exclusivity "SET VARIABLES"
     */
    void initialize_dependencies();

    /** Send notification message to administrator.
     * This method formats and sends a message describing system state change
     * via the notification script.  Included in the body of the message
     * are the variables that are not set to their default values.  If none of
     * the variables are set to their default values, the message body is
     * "All state variables at their default values."
     */
    void send_notification_message();

    /// Generation number of system state variables (#m_specs)a
    uint64_t m_generation;

    /// System state variables
    std::vector<SystemVariable::Spec> m_specs;

    /// Flag indicating if operation was generated by client request
    bool m_client_originated;
  };

  /// Smart pointer to OperationSetState
  typedef intrusive_ptr<OperationSetState> OperationSetStatePtr;

  /* @}*/

} // namespace Hypertable

#endif // HYPERTABLE_OPERATIONSETVARIABLES_H
