/* -*- c++ -*-
 * Copyright (C) 2007-2015 Hypertable, Inc.
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
/// Declarations for OperationAlterTable.
/// This file contains declarations for OperationAlterTable, an Operation class
/// for carrying out an <i>alter table</i> operation.

#ifndef Hypertable_Master_OperationAlterTable_h
#define Hypertable_Master_OperationAlterTable_h

#include <Hypertable/Master/Operation.h>

#include <Hypertable/Lib/Master/Request/Parameters/AlterTable.h>

#include <Common/StringExt.h>

namespace Hypertable {

  /// @addtogroup Master
  /// @{

  /// Carries out an alter table operation.
  class OperationAlterTable : public Operation {
  public:

    /** Constructor for constructing object from %MetaLog entry.
     * @param context %Master context
     * @param header_ %MetaLog header
     */
    OperationAlterTable(ContextPtr &context, const MetaLog::EntityHeader &header_);
    
    /** Constructor for constructing object from AsyncComm event 
     * Decodes #m_params from message payload and then initializes the
     * dependency graph state as follows:
     *   - <b>dependencies</b> - Dependency::INIT
     *   - <b>exclusivities</b> - %Table pathname
     * @param context %Master context
     * @param event %Event received from AsyncComm from client request
     */
    OperationAlterTable(ContextPtr &context, EventPtr &event);

    /** Destructor. */
    virtual ~OperationAlterTable() { }

    /** Carries out the alter table operation.
     * This method carries out the operation via the following states:
     *
     * <table>
     * <tr>
     * <th> State </th>
     * <th> Description </th>
     * </tr>
     * <tr>
     * <td> INITIAL </td>
     * <td><ul>
     *   <li> Maps supplied table name to a table identifier (#m_id) </li>
     *   <li> If supplied table name not found in name map, completes with
     *        Error::TABLE_NOT_FOUND </li>
     *   <li> If supplied table name is a directory, completes with
     *        Error::INVALID_ARGUMENT </li>
     *   <li> Otherwise, transitions to VALIDATE_SCHEMA </li>
     * </ul></td>
     * </tr>
     * <tr>
     * <td> VALIDATE_SCHEMA </td>
     * <td><ul>
     *   <li> Validates new schema and if bad, completes with
     *        Error::MASTER_BAD_SCHEMA </li>
     *   <li> Loads existing schema and checks to make sure new schema
     *        generation number is the same as the original generation
     *        number, if not completes with
     *        Error::MASTER_SCHEMA_GENERATION_MISMATCH. </li>
     *   <li> Otherwise, sets #m_parts to the index table that need to be
     *        created with a call to get_create_index_parts(), sets,
     *        dependencies to Dependency::METADATA, and transitions to
     *        CREATE_INDICES </li>
     * </ul></td>
     * </tr>
     * <tr>
     * <td>CREATE_INDICES</td>
     * <td><ul>
     * <li>If #m_parts specifies either a value or qualifier index, creates an
     *     OperationCreateTable sub operation to create index tables and stages
     *     it with a call to stage_subop()</li>
     * <li>Transition state to SCAN_METADATA</li>
     * <li>Persists this operation to MML and then returns</li>
     * </ul></td>
     * </tr>
     * <tr>
     * <td> SCAN_METADATA </td>
     * <td><ul>
     *   <li> Handles result of create table sub operation with a call to
     *        validate_subops(), returning on failure</li>
     *   <li> Scans the METADATA table and populates #m_servers to hold the set
     *        of servers that hold the table to be altered which are not in the
     *        #m_completed set. </li>
     *   <li> Dependencies are set to server names in #m_servers </li>
     *   <li> Transitions to the ISSUE_REQUESTS state </li>
     *   <li> Persists operation to MML and returns </li>
     * </ul></td>
     * </tr>
     * <tr>
     * <td> ISSUE_REQUESTS </td>
     * <td><ul>
     *   <li> Issues a alter table request to all servers in #m_servers and
     *        waits for their completion </li>
     *   <li> If there are any errors, for each server that was successful or
     *        returned with Error::TABLE_NOT_FOUND, the server name is added to
     *        #m_completed.  Dependencies are then set back to just
     *        Dependency::METADATA, the state is reset back to SCAN_METADATA,
     *        the operation is persisted to the MML, and the method returns</li>
     *   <li> Otherwise state is transitioned to UPDATE_HYPERSPACE and operation
     *        is persisted to MML </li>
     * </ul></td>
     * </tr>
     * <tr>
     * <td> UPDATE_HYPERSPACE </td>
     * <td><ul>
     *   <li> Sets #m_parts to the index table that need to be dropped with a
     *        call to get_drop_index_parts() </li>
     *   <li> Updates the schema attribute of table in hyperspace </li>
     *   <li> If any of the index tables need to be dropped, transitions to
     *        state SUSPEND_TABLE_MAINTENANCE, records this operation to
     *        the MML, and then returns </li>
     *   <li> Otherwise, transitions to COMPLETE and then returns </li>
     * </ul></td>
     * </tr>
     * <tr>
     * <td>SUSPEND_TABLE_MAINTENANCE</td>
     * <td><ul>
     * <li>Creates an OperationToggleMaintenance sub operation to turn
     *     maintenance off</li>
     * <li>Stages sub operation with a call to stage_subop()</li>
     * <li>Transition state to DROP_INDICES</li>
     * <li>Persists this operation to MML and then returns</li>
     * </ul></td>
     * </tr>
     * <tr>
     * <td>DROP_INDICES</td>
     * <td><ul>
     * <li>Handles result of toggle table maintenance sub operation with a
     *     call to validate_subops(), returning on failure</li>
     * <li>Creates an OperationDropTable sub operation to drop index
     *     tables</li>
     * <li>Stages sub operation with a call to stage_subop()</li>
     * <li>Transition state to RESUME_TABLE_MAINTENANCE</li>
     * <li>Persists this operation to MML and then returns</li>
     * </ul></td>
     * </tr>
     * <tr>
     * <td>RESUME_TABLE_MAINTENANCE</td>
     * <td><ul>
     * <li>Handles result of drop table sub operation with a call to
     *     validate_subops(), returning on failure</li>
     * <li>Creates an OperationToggleMaintenance sub operation to turn
     *     maintenance back on</li>
     * <li>Stages sub operation with a call to stage_subop()</li>
     * <li>Transition state to FINALIZE</li>
     * <li>Persists this operation to MML and then returns</li>
     * </ul></td>
     * </tr>
     * <tr>
     * <td>FINALIZE</td>
     * <td><ul>
     * <li>Handles result of the toggle maintenance sub operation with a call to
     *     fetch_and_validate_subop(), returning on failure</li>
     * <li>Calls complete_ok()</li>
     * </ul></td>
     * </tr>
     * </table>
     */
    virtual void execute();

    /** Returns name of operation ("OperationAlterTable")
     * @return %Operation name
     */
    virtual const String name();

    /** Returns descriptive label for operation
     * @return Descriptive label for operation
     */
    virtual const String label();

    /** Writes human readable representation of object to output stream.
     * @param os Output stream
     */
    virtual void display_state(std::ostream &os);

    /// Returns encoding version
    /// @return Encoding version
    uint8_t encoding_version_state() const override;

    /** Returns serialized state length.
     * This method returns the length of the serialized representation of the
     * object state.  See encode() for a description of the serialized format.
     * @return Serialized length
     */
    size_t encoded_length_state() const override;

    /** Writes serialized encoding of object state.
     * This method writes a serialized encoding of object state to the memory
     * location pointed to by <code>*bufp</code>.  The encoding has the
     * following format:
     * <table>
     *   <tr><th> Encoding </th><th> Description </th></tr>
     *   <tr><td> vstr </td><td> Pathname of table to compact </td></tr>
     *   <tr><td> vstr </td><td> New table schema </td></tr>
     *   <tr><td> vstr </td><td> %Table identifier </td></tr>
     *   <tr><td> i32  </td><td> Size of #m_completed </td></tr>
     *   <tr><td> vstr </td><td> <b>Foreach server</b> in #m_completed, server
     *                           name </td></tr>
     *   <tr><td> i32  </td><td> [VERSION 2] Size of #m_servers </td></tr>
     *   <tr><td> vstr </td><td> [VERSION 2] <b>Foreach server</b>
     *                           in #m_servers, server name </td></tr>
     *   <tr><td> TableParts </td><td> [VERSION 3] Index tables to be created
     *                                 or dropped </td></tr>
     * </table>
     * @param bufp Address of destination buffer pointer (advanced by call)
     */
    void encode_state(uint8_t **bufp) const override;

    /** Reads serialized encoding of object state.
     * This method restores the state of the object by decoding a serialized
     * representation of the state from the memory location pointed to by
     * <code>*bufp</code>.  See encode() for a description of the
     * serialized format.
     * @param version Encoding version
     * @param bufp Address of source buffer pointer (advanced by call)
     * @param remainp Amount of remaining buffer pointed to by <code>*bufp</code>
     * (decremented by call)
     */
    void decode_state(uint8_t version, const uint8_t **bufp, size_t *remainp) override;

    void decode_state_old(uint8_t version, const uint8_t **bufp, size_t *remainp) override;

  private:

    /// Gets schema objects.
    /// If either <code>original_schema</code> and <code>alter_schema</code> are
    /// are set to nullptr, this member function constructs and returns original
    /// schema object and alter schema object.  The original schema is
    /// constructed from XML read from Hyperspace and the alter schema is
    /// constructed from #m_schema.  If any errors are encountered,
    /// the operation is terminated with a call to complete_error() and
    //// <i>false</i> is returned.
    /// @param original_schema Reference to original schema
    /// @param alter_schema Reference to alter schema
    /// @return <i>true</i> on success, <i>false</i> on error signaling operaton
    /// has been terminated.
    bool get_schemas(SchemaPtr &original_schema, SchemaPtr &alter_schema);

    /// Determines which index tables to drop.
    /// Determines if either the value or qualifier index table was required in
    /// <code>original_schema</code> but not in <code>alter_schema</code>.  Each
    /// index type, for which this is the case, is marked in a TableParts object
    /// that is returned.
    /// @param original_schema Original schema
    /// @param alter_schema New altered schema
    /// @return TableParts describing index tables to drop
    TableParts get_drop_index_parts(SchemaPtr &original_schema,
                                    SchemaPtr &alter_schema);

    /// Determines which index tables to create.
    /// Determines if either the value or qualifier index table was not required
    /// in <code>original_schema</code> but is required in
    /// <code>alter_schema</code>.  Each index type, for which this is the case,
    /// is marked in a TableParts object that is returned.
    /// @param original_schema Original schema
    /// @param alter_schema New altered schema
    /// @return TableParts describing index tables to create
    TableParts get_create_index_parts(SchemaPtr &original_schema,
                                      SchemaPtr &alter_schema);

    /// Request parmaeters
    Lib::Master::Request::Parameters::AlterTable m_params;

    /// %Schema for the table
    String m_schema;

    /// %Table identifier
    String m_id;

    /// Set of participating range servers
    StringSet m_servers;

    /// Set of range servers that have completed operation
    StringSet m_completed;

    /// Index tables to be created or dropped
    TableParts m_parts;

  };

  /// @}

} // namespace Hypertable

#endif // Hypertable_Master_OperationAlterTable_h
