/* -*- c++ -*-
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
 * Declarations for OperationDropTable.
 * This file contains declarations for OperationDropTable, an Operation class
 * for dropping (removing) a table from the system.
 */

#ifndef HYPERTABLE_OPERATIONDROPTABLE_H
#define HYPERTABLE_OPERATIONDROPTABLE_H

#include "Common/StringExt.h"

#include "Operation.h"

namespace Hypertable {

  /** @addtogroup Master
   *  @{
   */

  /** Carries out a drop table operation. */
  class OperationDropTable : public Operation {
  public:

    /** Constructor.
     * Initializes object by passing MetaLog::EntityType::OPERATION_DROP_TABLE
     * to parent Operation constructor and sets #m_name to <code>name</code>
     * and #m_if_exists to <code>if_exists</code>.  It completes the
     * initialization with a call to initialize_dependencies().
     * @param context %Master context
     * @param name Pathname of table to drop
     * @param if_exists Flag indicating if operation should succeed if table
     * does not exist
     */
    OperationDropTable(ContextPtr &context, const String &name, bool if_exists);

    /** Constructor for constructing object from %MetaLog entry.
     * @param context %Master context
     * @param header_ %MetaLog header
     */
    OperationDropTable(ContextPtr &context, const MetaLog::EntityHeader &header_);

    /** Constructor for constructing object from AsyncComm event 
     * @param context %Master context
     * @param event %Event received from AsyncComm from client request
     */
    OperationDropTable(ContextPtr &context, EventPtr &event);

    /** Destructor. */
    virtual ~OperationDropTable() { }

    /** Carries out the drop table operation.
     * This method carries out the operation via the following states:
     *
     * <table>
     * <tr>
     * <th>State</th>
     * <th>Description</th>
     * </tr>
     * <tr>
     * <td>INITIAL</td>
     * <td><ul>
     * <li>Gets table ID from %Hyperspace and stores it in #m_id.  If mapping
     * does not exist in %Hyperspace, then if #m_if_exists is <i>true</i> it
     * completes successfully, otherwise it completes with error
     * Error::TABLE_NOT_FOUND</li>
     * <li>Checks %Hyperspace to see if a <i>value</i> index exists for the
     * table.  If so, it creates an OperationDropTable for the value index table
     * (<i>^table-name</i>) and makes the current operation dependent on it</li>
     * <li>Checks %Hyperspace to see if a <i>qualifier</i> index exists for the
     * table.  If so, it creates an OperationDropTable for the qualifier index
     * table (<i>^^table-name</i>) and makes the current operation dependent on
     * it</li>
     * <li>Transitions to UPDATE_HYPERSPACE state</li>
     * <li>Persists self to MML and returns</li>
     * </ul></td>
     * </tr>
     * <tr>
     * <td>UPDATE_HYPERSPACE</td>
     * <td><ul>
     * <li>Drops the name/id mapping for table in %Hyperspace</li>
     * <li>Dependencies set to Dependency::METADATA and "<table-id> move range",
     * the latter causing the drop table operation to wait for all in-progress
     * MoveRange operations on the table to complete</li>
     * <li>Transitions to SCAN_METADATA state</li>
     * <li>Persists self to MML and returns</li>
     * </ul></td>
     * </tr>
     * <tr>
     * <td>SCAN_METADATA</td>
     * <td><ul>
     * <li>Scans the METADATA table and populates #m_servers to hold the set
     * of servers that hold the table to be dropped which are not in the
     * #m_completed set.</li>
     * <li>Dependencies are set to server names in #m_servers</li>
     * <li>Transitions to ISSUE_REQUESTS state</li>
     * <li>Persists self to MML and returns</li>
     * </ul></td>
     * </tr>
     * <tr>
     * <td>ISSUE_REQUESTS</td>
     * <td><ul>
     * <li>Issues a drop table request to all servers in #m_servers and waits
     * for their completion</li>
     * <li>If there are any errors, for each server that was successful or
     * returned with Error::TABLE_NOT_FOUND,
     * the server name is added to #m_completed.  Dependencies are then set back
     * to Dependency::METADATA and "<table-id> move range, the state is reset
     * back to SCAN_METADATA, the operation is persisted to the MML, and the
     * method returns</li>
     * <li>Otherwise the table is purged from the monitoring subsystem and state
     * is transitioned to COMPLETED</li>
     * </ul></td>
     * </tr>
     */
    virtual void execute();

    /** Returns name of operation ("OperationDropTable")
     * @return %Operation name
     */
    virtual const String name();

    /** Returns descriptive label for operation.
     * This method returns a descriptive label for the operation which
     * is constructed as the string "DropTable " followed by the name of the
     * table being dropped.
     * @return Descriptive label for operation
     */
    virtual const String label();

    /** Writes human readable representation of object to output stream.
     * The string returned has the following format:
     * <pre>
     *  name=<TableName> id=<TableId>
     * </pre>
     * @param os Output stream
     */
    virtual void display_state(std::ostream &os);

    virtual uint16_t encoding_version() const;

    /** Returns serialized state length.
     * This method returns the length of the serialized representation of the
     * object state.  See encode() for a description of the serialized format.
     * @return Serialized length
     */
    virtual size_t encoded_state_length() const;

    /** Writes serialized encoding of object state.
     * This method writes a serialized encoding of object state to the memory
     * location pointed to by <code>*bufp</code>.  The encoding has the
     * following format:
     * <table>
     *   <tr>
     *   <th>Encoding</th><th>Description</th>
     *   </tr>
     *   <tr>
     *   <td>bool</td><td><i>if exists</i> flag</td>
     *   </tr>
     *   <tr>
     *   <td>vstr</td><td>%Table name</td>
     *   </tr>
     *   <tr>
     *   <td>vstr</td><td>%Table identifier</td>
     *   </tr>
     *   <tr>
     *   <td>i32</td><td>Size of #m_completed</td>
     *   </tr>
     *   <tr>
     *   <td>vstr</td><td><b>foreach</b> server in #m_completed, server name</td>
     *   </tr>
     *   <tr>
     *   <td>i32</td><td>[VERSION 2] Size of #m_servers</td>
     *   </tr>
     *   <tr>
     *   <td>vstr</td><td>[VERSION 2] <b>foreach</b> server in #m_servers, server name</td>
     *   </tr>
     * </table>
     * @param bufp Address of destination buffer pointer (advanced by call)
     */
    virtual void encode_state(uint8_t **bufp) const;

    /** Decodes state from serialized object.
     * This method restores the state of the object by decoding a serialized
     * representation of the state from the memory location pointed to by
     * <code>*bufp</code>.  This method is implemented by first calling
     * decode_request() to decode and load the #m_if_exists and #m_name
     * variables and then decodes and loads the remaining variables.
     * See encode() for a description of the serialized format.  
     * @param bufp Address of source buffer pointer (advanced by call)
     * @param remainp Amount of remaining buffer pointed to by <code>*bufp</code>
     * (decremented by call)
     */
    virtual void decode_state(const uint8_t **bufp, size_t *remainp);

    /** Decodes request portion ofstate from serialized object.
     * This method decodes and loads the #m_if_exists and #m_name variables
     * which are the first two variables of the encoding.  See encode() for a
     * description of the serialized format.
     * @param bufp Address of source buffer pointer (advanced by call)
     * @param remainp Amount of remaining buffer pointed to by <code>*bufp</code>
     * (decremented by call)
     */    
    virtual void decode_request(const uint8_t **bufp, size_t *remainp);

  private:

    /** Initializes dependency graph state.
     * This method initializes the dependency graph state as follows:
     *
     *   - <b>dependencies</b> - Dependency::INIT
     *   - <b>exclusivities</b> - %Table pathname
     */
    void initialize_dependencies();

    /// Pathtname of table to drop
    String m_name;

    /// If table being dropped does not exist, succeed without generating error
    bool m_if_exists;

    /// %Table ID string
    String m_id;

    /// Set of participating range servers
    StringSet m_servers;

    /// %Range servers for which drop table operation has completed successfuly
    StringSet m_completed;
  };

  /**  @} */

} // namespace Hypertable

#endif // HYPERTABLE_OPERATIONDROPTABLE_H
