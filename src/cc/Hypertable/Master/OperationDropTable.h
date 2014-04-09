/* -*- c++ -*-
 * Copyright (C) 2007-2014 Hypertable, Inc.
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

/// @file
/// Declarations for OperationDropTable.
/// This file contains declarations for OperationDropTable, an Operation class
/// for dropping (removing) a table from the system.

#ifndef HYPERTABLE_OPERATIONDROPTABLE_H
#define HYPERTABLE_OPERATIONDROPTABLE_H

#include <Hypertable/Master/Operation.h>

#include <Hypertable/Lib/TableParts.h>

#include <Common/StringExt.h>

#include <vector>

namespace Hypertable {

  /// @addtogroup Master
  /// @{

  /// Carries out a drop table operation.
  class OperationDropTable : public Operation {
  public:

    /// Constructor.
    /// Initializes object by passing MetaLog::EntityType::OPERATION_DROP_TABLE
    /// to parent Operation constructor and sets #m_name to <code>name</code>
    /// and #m_if_exists to <code>if_exists</code>.  It completes the
    /// initialization with a call to initialize_dependencies().
    /// @param context %Master context
    /// @param name Pathname of table to drop
    /// @param if_exists Flag indicating if operation should succeed if table
    /// does not exist
    /// @param parts Which parts of the table to drop
    OperationDropTable(ContextPtr &context, const String &name, bool if_exists,
                       TableParts parts);

    /// Constructor with MML entity.
    /// @param context %Master context
    /// @param header_ %MetaLog header
    OperationDropTable(ContextPtr &context, const MetaLog::EntityHeader &header_);

    /// Constructor with client request.
    /// @param context %Master context
    /// @param event %Event received from AsyncComm from client request
    OperationDropTable(ContextPtr &context, EventPtr &event);

    /// Destructor.
    virtual ~OperationDropTable() { }

    /// Carries out the drop table operation.
    /// This method carries out the operation via the following states:
    ///
    /// <table>
    /// <tr>
    /// <th>State</th>
    /// <th>Description</th>
    /// </tr>
    /// <tr>
    /// <td>INITIAL</td>
    /// <td><ul>
    /// <li>Gets table ID from %Hyperspace and stores it in #m_id.  If mapping
    /// does not exist in %Hyperspace, then if #m_if_exists is <i>true</i> it
    /// completes successfully, otherwise it completes with error
    /// Error::TABLE_NOT_FOUND</li>
    /// <li>Checks %Hyperspace to see if a <i>value</i> index exists for the
    /// table.  If so, it creates an OperationDropTable for the value index table
    /// (<i>^table-name</i>) and makes the current operation dependent on it</li>
    /// <li>Checks %Hyperspace to see if a <i>qualifier</i> index exists for the
    /// table.  If so, it creates an OperationDropTable for the qualifier index
    /// table (<i>^^table-name</i>) and makes the current operation dependent on
    /// it</li>
    /// <li>Transitions to DROP_VALUE_INDEX</li>
    /// <li>Persists self to MML and returns</li>
    /// </ul></td>
    /// </tr>
    /// <tr>
    /// <td>DROP_VALUE_INDEX</td>
    /// <td><ul>
    /// <li>If value index is not specified in #m_parts or the value index table
    ///     does not exist in Hyperspace, state is transitioned to
    ///     DROP_QUALIFIER_INDEX and function drops through to next state</li>
    /// <li>Creates OperationDropTable sub operation for value index table</li>
    /// <li>Stages sub operation with call to stage_subop() with dependency string
    ///     #m_name + "-drop-index"</li>
    /// <li>Transitions to state DROP_QUALIFIER_INDEX</li>
    /// <li>Persists operation and sub operation to MML and returns</li>
    /// </tr>
    /// <tr>
    /// <td>DROP_QUALIFIER_INDEX</td>
    /// <td><ul>
    /// <li>Handles result of value index dropping sub operation with a call to
    ///     fetch_and_validate_subop(), returning if it failed</li>
    /// <li>If qualifier index is not specified in #m_parts or the qualifier
    ///     index table does not exist in Hyperspace, state is transitioned to
    ///     UPDATE_HYPERSPACE, this and the completed sub op are persisted to
    ///     the MML and then drops through to next state</li>
    /// <li>Creates OperationDropTable sub operation for the qualifier index
    ///     table</li>
    /// <li>Stages sub operation with call to stage_subop() with dependency string
    ///     #m_name + "-drop-qualifier-index"</li>
    /// <li>Transitions to state UPDATE_HYPERSPACE</li>
    /// <li>Persists operation and sub operation to MML and returns</li>
    /// </tr>
    /// <tr>
    /// <td>UPDATE_HYPERSPACE</td>
    /// <td><ul>
    /// <li>Drops the name/id mapping for table in %Hyperspace</li>
    /// <li>Dependencies set to Dependency::METADATA and "<table-id> move range",
    /// the latter causing the drop table operation to wait for all in-progress
    /// MoveRange operations on the table to complete</li>
    /// <li>Transitions to SCAN_METADATA state</li>
    /// <li>Persists self to MML and returns</li>
    /// </ul></td>
    /// </tr>
    /// <tr>
    /// <td>SCAN_METADATA</td>
    /// <td><ul>
    /// <li>Scans the METADATA table and populates #m_servers to hold the set
    /// of servers that hold the table to be dropped which are not in the
    /// #m_completed set.</li>
    /// <li>Dependencies are set to server names in #m_servers</li>
    /// <li>Transitions to ISSUE_REQUESTS state</li>
    /// <li>Persists self to MML and returns</li>
    /// </ul></td>
    /// </tr>
    /// <tr>
    /// <td>ISSUE_REQUESTS</td>
    /// <td><ul>
    /// <li>Issues a drop table request to all servers in #m_servers and waits
    /// for their completion</li>
    /// <li>If there are any errors, for each server that was successful or
    /// returned with Error::TABLE_NOT_FOUND,
    /// the server name is added to #m_completed.  Dependencies are then set back
    /// to METADATA and #m_id + " move range", the state is reset
    /// back to SCAN_METADATA, the operation is persisted to the MML, and the
    /// method returns</li>
    /// <li>Otherwise the table is purged from the monitoring subsystem and state
    /// is transitioned to COMPLETED</li>
    /// </ul></td>
    /// </tr>
    virtual void execute();

    /// Returns name of operation ("OperationDropTable")
    /// @return %Operation name
    virtual const String name();

    /// Returns descriptive label for operation.
    /// This method returns a descriptive label for the operation which
    /// is constructed as the string "DropTable " followed by the name of the
    /// table being dropped.
    /// @return Descriptive label for operation
    virtual const String label();

    /// Writes human readable representation of object to output stream.
    /// The string returned has the following format:
    /// <pre>
    ///  name=<TableName> id=<TableId>
    /// </pre>
    /// @param os Output stream
    virtual void display_state(std::ostream &os);

    virtual uint16_t encoding_version() const;

    /// Returns serialized state length.
    /// This method returns the length of the serialized representation of the
    /// object state.
    /// @return Serialized length.
    /// @see encode() for a description of the serialized %format.
    virtual size_t encoded_state_length() const;

    /// Writes serialized encoding of object state.
    /// This method writes a serialized encoding of object state to the memory
    /// location pointed to by <code>*bufp</code>.  The encoding has the
    /// following format:
    /// <table>
    ///   <tr>
    ///   <th>Encoding</th><th>Description</th>
    ///   </tr>
    ///   <tr>
    ///   <td>bool</td><td><i>if exists</i> flag</td>
    ///   </tr>
    ///   <tr>
    ///   <td>vstr</td><td>%Table name</td>
    ///   </tr>
    ///   <tr>
    ///   <td>vstr</td><td>%Table identifier</td>
    ///   </tr>
    ///   <tr>
    ///   <td>i32</td><td>Size of #m_completed</td>
    ///   </tr>
    ///   <tr>
    ///   <td>vstr</td><td><b>foreach</b> server in #m_completed, server name</td>
    ///   </tr>
    ///   <tr>
    ///   <td>i32</td><td>[VERSION 2] Size of #m_servers</td>
    ///   </tr>
    ///   <tr>
    ///   <td>vstr</td><td>[VERSION 2] <b>foreach</b> server in #m_servers, server name</td>
    ///   </tr>
    ///   <tr>
    ///   <td>TableParts</td><td>[VERSION 3] %Table parts to create (#m_parts)
    ///   </td>
    ///   </tr>
    ///   <tr>
    ///   <td>i64</td><td>[VERSION 3] Sub operation hash code
    ///       (#m_subop_hash_code)</td>
    ///   </tr>
    /// </table>
    /// @param bufp Address of destination buffer pointer (advanced by call)
    virtual void encode_state(uint8_t **bufp) const;

    /// Decodes state from serialized object.
    /// This method restores the state of the object by decoding a serialized
    /// representation of the state from the memory location pointed to by
    /// <code>*bufp</code>.  This method is implemented by first calling
    /// decode_request() to decode and load the #m_if_exists and #m_name
    /// variables and then decodes and loads the remaining variables.
    /// @param bufp Address of source buffer pointer (advanced by call)
    /// @param remainp Amount of remaining buffer pointed to by <code>*bufp</code>
    /// (decremented by call)
    /// @see encode() for a description of the serialized %format
    virtual void decode_state(const uint8_t **bufp, size_t *remainp);

    /// Decodes request portion ofstate from serialized object.
    /// This method decodes and loads the #m_if_exists and #m_name variables
    /// which are the first two variables of the encoding.
    /// @param bufp Address of source buffer pointer (advanced by call)
    /// @param remainp Amount of remaining buffer pointed to by <code>*bufp</code>
    /// (decremented by call)
    /// @see encode() for a description of the serialized %format
    virtual void decode_request(const uint8_t **bufp, size_t *remainp);

  private:

    /// Initializes dependency graph state.
    /// This method initializes the dependency graph state as follows:
    ///
    ///   - <b>dependencies</b> - INIT
    ///   - <b>exclusivities</b> - %Table pathname
    ////
    void initialize_dependencies();

    /// Fetchs and handles the result of sub operation.
    /// If #m_subop_hash_code is non-zero, a sub operation is outstanding and
    /// this member function will fetch it and process it as follows:
    ///   - Fetches the sub operation from the ReferenceManager
    ///   - Adds remove approvals 0x01 to the sub operation
    ///   - Sets #m_subop_hash_code to zero
    ///   - If sub operation error code is non-zero, completes overall operaton
    ///     with a call to complete_error(), and returns <i>false</i>.
    ///   - Otherwise, sub operation is added to <code>entities</code>
    /// @param entities Reference to vector of entites to hold sucessfully
    /// completed sub operation
    /// @return <i>true</i> if no sub operation is outstanding or the sub
    /// operation completed without error, <i>false</i> otherwise.
    bool fetch_and_validate_subop(vector<Entity *> &entities);

    /// Stages a sub operation for execution.
    /// This member function does the following:
    ///   - Adds <code>dependency_string</code> to sub operation's set of
    ///     obstructions
    ///   - Sets sub operation remove approval mask to 0x01
    ///   - Adds sub operation to ReferenceManager
    ///   - Adds <code>dependency_string</code> to parent (this) operation's set
    ///     of dependencies
    ///   - Pushes sub operation onto #m_sub_ops
    ///   - Sets #m_subop_hash_code to sub operation's hash code
    /// @param opartion Sub operation
    /// @param dependency_string %Dependency string used to serialize sub
    /// operation with parent operation
    void stage_subop(Operation *operation, std::string dependency_string);

    /// Pathtname of table to drop
    String m_name;

    /// %Table ID string
    String m_id;

    /// Set of participating range servers
    StringSet m_servers;

    /// %Range servers for which drop table operation has completed successfuly
    StringSet m_completed;

    /// If table being dropped does not exist, succeed without generating error
    bool m_if_exists {};

    /// Specification for which parts of table to drop
    TableParts m_parts {TableParts::ALL};

    /// Hash code for currently outstanding sub operation
    int64_t m_subop_hash_code {};
  };

  /// @}

} // namespace Hypertable

#endif // HYPERTABLE_OPERATIONDROPTABLE_H
