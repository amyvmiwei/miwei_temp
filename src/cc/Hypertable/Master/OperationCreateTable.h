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
/// Declarations for OperationCreateTable.
/// This file contains declarations for OperationCreateTable, an Operation class
/// for creating a table.

#ifndef Hypertable_Master_OperationCreateTable_h
#define Hypertable_Master_OperationCreateTable_h

#include <Hypertable/Master/Operation.h>

#include <Hypertable/Lib/Master/Request/Parameters/CreateTable.h>
#include <Hypertable/Lib/TableParts.h>

namespace Hypertable {

  /// @addtogroup Master
  /// @{

  /// Carries out a <i>create table</i> operation.
  /// This class is responsible for creating a new table, which involves,
  /// creating the table in Hyperspace, loading the initial range for the table,
  /// and optionally creating the associated value and qualifier index tables if
  /// required.
  class OperationCreateTable : public Operation {
  public:

    /// Constructor.
    /// Initializes #m_params with <code>name</code> and <code>schema</code>,
    /// and initializes #m_parts with <code>parts</code>.
    /// @param context %Master context
    /// @param name Full pathname of table to create
    /// @param schema %Table schema
    /// @param parts Which parts of the table to create
    OperationCreateTable(ContextPtr &context, const String &name,
                         const String &schema, TableParts parts);

    /// Constructor with MML entity.
    /// @param context %Master context
    /// @param header %MetaLog header
    OperationCreateTable(ContextPtr &context,
                         const MetaLog::EntityHeader &header) :
      Operation(context, header) { }

    /// Constructor with client request.
    /// Initializes base class constructor, and then initializes the member
    /// variables as follows:
    ///   - Decodes request parameters
    ///   - Table name is added as an exclusivity
    ///   - Adds METADATA and SYSTEM as dependencies
    ///   - If table is a system table, INIT is added as a dependency
    /// @param context %Master context
    /// @param event %Event received from AsyncComm from client request
    OperationCreateTable(ContextPtr &context, EventPtr &event);

    /// Destructor.
    virtual ~OperationCreateTable() { }

    /// Carries out the create table operation.
    /// This method carries out the operation via the states described below.
    /// If #m_parts indicates that the primary table is not being created and
    /// the entry state is INITIAL, the state is set to CREATE_INDEX.
    ///
    /// <table>
    /// <tr>
    /// <th>State</th>
    /// <th>Description</th>
    /// </tr>
    /// <tr>
    /// <td>INITIAL</td>
    /// <td><ul>
    /// <li>Verifies that a table with the same name does not already exist in
    ///     Hyperspace and completes with error Error::NAME_ALREADY_IN_USE if it
    ///     does.</li>
    /// <li>Obtains current timestamp and uses it as the schema generation
    ///     number.</li>
    /// <li>Transitions to state ASSIGN_ID</li>
    /// <li>Persists operation to MML and returns</li>
    /// </ul></td>
    /// </tr>
    /// <tr>
    /// <td>ASSIGN_ID</td>
    /// <td><ul>
    /// <li>Creates table in Hyperspace</li>
    /// <li>Updates #m_parts to reflect indices that actually exist in the
    ///     schema</li>
    /// <li>Transitions to the CREATE_INDEX</li>
    /// </ul></td>
    /// </tr>
    /// <tr>
    /// <td>CREATE_INDEX</td>
    /// <td><ul>
    /// <li>If value index not specified in #m_parts, state is transitioned to
    ///     CREATE_QUALIFIER_INDEX
    /// <br>... otherwise ...</li>
    /// <li>Prepares the value index with call to Utility::prepare_index()</li>
    /// <li>Creates OperationCreateTable sub operation for value index table</li>
    /// <li>Stages sub operation with call to stage_subop()</li>
    /// <li>Transitions to state CREATE_QUALIFIER_INDEX</li>
    /// <li>Persists operation with call to record_state() and returns</li>
    /// </ul></td>
    /// </tr>
    /// <tr>
    /// <td>CREATE_QUALIFIER_INDEX</td>
    /// <td><ul>
    /// <li>Handles result of value index sub operation with a call to
    ///     validate_subops(), returning if it failed</li>
    /// <li>If qualifier index not specified in #m_parts, state is transitioned
    ///     to WRITE_METADATA, the operation is persisted to the MML, and drops
    ///     through to the next state.
    /// <br>... otherwise ...</li>
    /// <li>Prepares the qualifier index with call to
    ///     Utility::prepare_index()</li>
    /// <li>Creates OperationCreateTable sub operation for qualifier index
    ///     table</li>
    /// <li>Stages sub operation with call to stage_subop()</li>
    /// <li>Transitions to state WRITE_METADATA</li>
    /// <li>Persists operation with a call to record_state() and returns</li>
    /// </ul></td>
    /// </tr>
    /// <tr>
    /// <td>WRITE_METADATA</td>
    /// <td><ul>
    /// <li>Handles result of qualifier index sub operation with a call to
    ///     validate_subops(), returning on failure</li>
    /// <li>If primary table is not specified in #m_parts, complete_ok() is
    ///     called and the function returns</li>
    /// <li>Utility::create_table_write_metadata() is called</li>
    /// <li>The operation's dependencies are set to SERVERS, METADATA, and
    ///     SYSTEM and an obstruction ("OperationMove " + range) is added.</li>
    /// <li>Transitions to state ASSIGN_LOCATION</li>
    /// <li>Persists operation with a call to record_state() and returns</li>
    /// </ul></td>
    /// </tr>
    /// <tr>
    /// <td>ASSIGN_LOCATION</td>
    /// <td><ul>
    /// <li>Location for initial range is obtained via
    ///     BalancePlanAuthority::get_balance_destination() and stored to
    ///     #m_location</li>
    /// <li>The operation's dependencies are set to METADATA, and SYSTEM and an
    ///     obstruction ("OperationMove " + range) is added.</li>
    /// <li>Transitions to state LOAD_RANGE</li>
    /// <li>Operation is persisted to MML and function returns</li>
    /// </ul></td>
    /// </tr>
    /// <tr>
    /// <td>LOAD_RANGE</td>
    /// <td><ul>
    /// <li>Initial range is loaded with a call to
    ///     Utility::create_table_load_range()</li>
    /// <li>If an Exception is thrown during the loading of the range, it is
    ///     caught, the operation sleeps for 5 seconds, the state is
    ///     transitioned to ASSIGN_LOCATION, and the function returns</li>
    /// <li>Otherwise, on success the state is transitioned to ACKNOWLEDGE</li>
    /// <li>Operation is persisted to MML and function returns</li>
    /// </ul></td>
    /// </tr>
    /// <tr>
    /// <td>ACKNOWLEDGE</td>
    /// <td><ul>
    /// <li>Range is acknowledged with a call to
    ///     Utility::create_table_acknowledge_range()</li>
    /// <li>If an Exception is thrown during the acknowledgement of the range,
    ///     it is caught, the operation sleeps for 5 seconds, then assuming that
    ///     the range was moved, its new location is obtained with a call to
    ///     BalancePlanAuthority::get_balance_destination() and the function
    ///     returns</li>
    /// <li>Otherwise, on success the state is transitioned to FINALIZE and the
    ///     function drops through to the next state</li>
    /// </ul></td>
    /// </tr>
    /// <tr>
    /// <td>FINALIZE</td>
    /// <td><ul>
    /// <li>BalancePlanAuthority::balance_move_complete() is called</li>
    /// <li>The "x" attribute is set on the table's ID file in Hyperspace</li>
    /// <li>The operation is complted with a call to complete_ok()</li>
    /// </ul></td>
    /// </tr>
    /// </table>
    virtual void execute();

    /// Returns name of operation
    /// Returns name of operation (<code>OperationCreateTable</code>)
    /// @return Name of operation
    virtual const String name();

    /// Returns label for operation
    /// Returns string "CreateTable <tablename>" 
    /// Label for operation
    virtual const String label();

    /// Writes human readable representation of object to output stream.
    /// @param os Output stream
    virtual void display_state(std::ostream &os);

    /// Returns encoding version of serialization format.
    /// @return Encoding version of serialization format.
    uint8_t encoding_version_state() const override;

    /// Returns serialized state length.
    /// This method returns the length of the serialized representation of the
    /// object state.
    /// @return Serialized length
    /// @see encode() for a description of the serialized %format.
    size_t encoded_length_state() const override;
    
    /// Writes serialized encoding of object state.
    /// This method writes a serialized encoding of object state to the memory
    /// location pointed to by <code>*bufp</code>.  The encoding has the
    /// following format:
    /// <table>
    ///   <tr>
    ///   <th>Encoding</th><th>Description</th>
    ///   </tr>
    ///   <tr>
    ///   <td>vstr</td><td>%Table name</td>
    ///   </tr>
    ///   <tr>
    ///   <td>vstr</td><td>%Table schema (#m_schema)</td>
    ///   </tr>
    ///   <tr>
    ///   <td>TableIdentifier</td><td>%Table identifier (#m_table)</td>
    ///   </tr>
    ///   <tr>
    ///   <td>vstr</td><td>Proxy name of server to hold initial range
    ///       (#m_location)</td>
    ///   </tr>
    ///   <tr>
    ///   <td>TableParts</td><td>[VERSION 2] %Table parts to create (#m_parts)
    ///   </td>
    ///   </tr>
    /// </table>
    /// @param bufp Address of destination buffer pointer (advanced by call)
    void encode_state(uint8_t **bufp) const override;

    /// Reads serialized encoding of object state.
    /// This method restores the state of the object by decoding a serialized
    /// representation of the state from the memory location pointed to by
    /// <code>*bufp</code>.
    /// @param version Encoding version
    /// @param bufp Address of source buffer pointer (advanced by call)
    /// @param remainp Amount of remaining buffer pointed to by <code>*bufp</code>
    /// (decremented by call)
    /// @see encode() for a description of the serialized %format.
    void decode_state(uint8_t version, const uint8_t **bufp, size_t *remainp);

    void decode_state_old(uint8_t version, const uint8_t **bufp, size_t *remainp);

  private:

    /// Request parmaeters
    Lib::Master::Request::Parameters::CreateTable m_params;

    /// %Table identifier
    TableIdentifierManaged m_table;

    /// %Schema for the table
    String m_schema;

    /// %Proxy name of server to hold initial range
    String m_location;

    /// Which parts of table to create
    TableParts m_parts {TableParts::ALL};

  };

  /// @}

} // namespace Hypertable

#endif // Hypertable_Master_OperationCreateTable_h
