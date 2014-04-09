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
/// Declarations for OperationRecreateIndexTables.
/// This file contains declarations for OperationRecreateIndexTables, an
/// Operation class for reconstructing a table's index tables.

#ifndef Hypertable_Master_OperationRecreateIndexTables_h
#define Hypertable_Master_OperationRecreateIndexTables_h

#include "Operation.h"

#include <Hypertable/Lib/TableParts.h>

#include <Common/String.h>

namespace Hypertable {

  /// @addtogroup Master
  /// @{

  /// Reconstructs a table's index tables.
  class OperationRecreateIndexTables : public Operation {
  public:

    /// Constructor.
    /// Initializes object by passing
    /// MetaLog::EntityType::OPERATION_RECREATE_INDEX_TABLES to parent Operation
    /// constructor and then initializes member variables as follows:
    ///   - Sets #m_name to canonicalized <code>table_name</code>
    ///   - Sets #m_parts to <code>table_parts</code>
    ///   - Adds #m_name as an exclusivity
    /// @param context %Master context
    /// @param table_name Pathname of table
    /// @param table_parts Describes which index tables to re-create
    OperationRecreateIndexTables(ContextPtr &context, std::string table_name,
                                 TableParts table_parts);

    /// Constructor with MML entity.
    /// @param context %Master context
    /// @param header %MetaLog header
    OperationRecreateIndexTables(ContextPtr &context,
                                 const MetaLog::EntityHeader &header);

    /// Constructor with client request.
    /// Initializes base class constructor and then initializes member variables
    /// as follows:
    ///   - Decodes request with a call to decode_request()
    ///   - Cannonicalizes #m_name with a call to 
    ///     Utility::canonicalize_pathname().
    ///   - Adds #m_name as an exclusivity
    /// @param context %Master context
    /// @param event %Event received from AsyncComm from client request
    OperationRecreateIndexTables(ContextPtr &context, EventPtr &event);
    
    /// Destructor.
    virtual ~OperationRecreateIndexTables() { }

    /// Carries out recreate index tables operation.
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
    /// <li>Reads table schema from Hyperspace</li>
    /// <li>Combines information about existing index tables with the specified
    ///     index tables passed in via #m_parts and stores the result to
    ///     #m_parts</li>
    /// <li>If no index tables are specified after previous step,
    ///     complete_error() is called with Error::NOTHING_TO_DO</li>
    /// <li>State is transitioned to SUSPEND_TABLE_MAINTENANCE</li>
    /// <li>Operation is persisted to MML and drops through</li>
    /// </ul></td>
    /// </tr>
    /// <tr>
    /// <td>SUSPEND_TABLE_MAINTENANCE</td>
    /// <td><ul>
    /// <li>Creates OperationToggleMaintenance sub operation to turn maintenance
    ///     off</li>
    /// <li>Stages sub operation with a call to stage_subop()</li>
    /// <li>Transition state to DROP_INDICES</li>
    /// <li>Persists this operation and sub operation to MML and then
    ///     returns</li>
    /// </ul></td>
    /// </tr>
    /// <tr>
    /// <td>DROP_INDICES</td>
    /// <td><ul>
    /// <li>Handles result of toggle maintenance sub operation with a call to
    ///     fetch_and_validate_subop(), returning on failure</li>
    /// <li>Creates OperationDropTable sub operation to drop index tables</li>
    /// <li>Stages sub operation with a call to stage_subop()</li>
    /// <li>Transition state to CREATE_INDICES</li>
    /// <li>Persists this operation and sub operation to MML and then
    ///     returns</li>
    /// </ul></td>
    /// </tr>
    /// <tr>
    /// <td>CREATE_INDICES</td>
    /// <td><ul>
    /// <li>Handles result of drop table sub operation with a call to
    ///     fetch_and_validate_subop(), returning on failure</li>
    /// <li>Fetches schema from Hyperspace and creates an OperationCreateTable
    ///     sub operation to create index tables</li>
    /// <li>Stages sub operation with a call to stage_subop()</li>
    /// <li>Transition state to RESUME_TABLE_MAINTENANCE</li>
    /// <li>Persists this operation and sub operation to MML and then
    ///     returns</li>
    /// </ul></td>
    /// </tr>
    /// <tr>
    /// <td>RESUME_TABLE_MAINTENANCE</td>
    /// <td><ul>
    /// <li>Handles result of create table sub operation with a call to
    ///     fetch_and_validate_subop(), returning on failure</li>
    /// <li>Creates OperationToggleMaintenance sub operation to turn maintenance
    ///     back on</li>
    /// <li>Stages sub operation with a call to stage_subop()</li>
    /// <li>Transition state to FINALIZE</li>
    /// <li>Persists this operation and sub operation to MML and then
    ///     returns</li>
    /// </ul></td>
    /// </tr>
    /// <tr>
    /// <td>FINALIZE</td>
    /// <td><ul>
    /// <li>Handles result of toggle table maintenance sub operation with a call
    ///     to fetch_and_validate_subop(), returning on failure</li>
    /// <li>%Operation is completed with a call to complete_ok()</li>
    /// </ul></td>
    /// </tr>
    /// </table>
    virtual void execute();

    /// Returns name of operation.
    /// Returns name of operation "OperationRecreateIndexTables"
    /// @return %Operation name
    virtual const String name();

    /// Returns descriptive label for operation
    /// @return Descriptive label for operation
    virtual const String label();

    /// Writes human readable representation of object to output stream.
    /// @param os Output stream
    virtual void display_state(std::ostream &os);

    /// Returns encoding version of serialization format.
    /// @return Encoding version of serialization format.
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
    ///   <td>vstr</td><td>%Table name (#m_name)</td>
    ///   </tr>
    ///   <tr>
    ///   <td>TableParts</td><td>Specification for which index tables to
    ///       re-create (#m_parts)</td>
    ///   </tr>
    ///   <tr>
    ///   <td>i64</td><td>Sub operation hash code (#m_subop_hash_code)</td>
    ///   </tr>
    /// </table>
    /// @param bufp Address of destination buffer pointer (advanced by call)
    virtual void encode_state(uint8_t **bufp) const;

    /// Reads serialized encoding of object state.
    /// This method restores the state of the object by decoding a serialized
    /// representation of the state from the memory location pointed to by
    /// <code>*bufp</code>.
    /// @param bufp Address of source buffer pointer (advanced by call)
    /// @param remainp Amount of remaining buffer pointed to by <code>*bufp</code>
    /// (decremented by call)
    /// @see encode() for a description of the serialized %format.
    virtual void decode_state(const uint8_t **bufp, size_t *remainp);

    /// Decodes a request that triggered the operation.
    /// This method decodes a request sent from a client that caused this
    /// object to get created.  The encoding has the following format:
    /// <table>
    ///   <tr>
    ///   <th>Encoding</th><th>Description</th>
    ///   </tr>
    ///   <tr>
    ///   <td>vstr</td><td>%Table name (#m_name)</td>
    ///   </tr>
    ///   <tr>
    ///   <td>TableParts</td><td>Specification of which index tables to
    ///       re-create (#m_parts)</td>
    ///   </tr>
    /// </table>
    /// @param bufp Address of source buffer pointer (advanced by call)
    /// @param remainp Amount of remaining buffer pointed to by <code>*bufp</code>
    /// (decremented by call)
    virtual void decode_request(const uint8_t **bufp, size_t *remainp);

  private:

    bool fetch_schema(std::string &schema);

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
    ///   - Sets local variable <code>dpendency_string</code> to
    ///     "RECREATE INDEX TABLES subop " + operation->hash_code()
    ///   - Adds <code>dependency_string</code> to sub operation's set of
    ///     obstructions
    ///   - Sets sub operation remove approval mask to 0x01
    ///   - Adds sub operation to ReferenceManager
    ///   - Adds <code>dependency_string</code> to parent (this) operation's set
    ///     of dependencies
    ///   - Pushes sub operation onto #m_sub_ops
    ///   - Sets #m_subop_hash_code to sub operation's hash code
    /// @param opartion Sub operation
    void stage_subop(Operation *operation);

    /// %Table name
    std::string m_name;

    /// Specification for which index tables to re-create
    TableParts m_parts {0};

    /// Hash code for currently outstanding sub operation
    int64_t m_subop_hash_code {};
  };

  /// @}

} // namespace Hypertable

#endif // Hypertable_Master_OperationRecreateIndexTables_h
