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
/// Declarations for OperationToggleTableMaintenance.
/// This file contains declarations for OperationToggleTableMaintenance, an
/// Operation class for toggling maintenance for a table either on or off.

#ifndef Hypertable_Master_OperationToggleTableMaintenance_h
#define Hypertable_Master_OperationToggleTableMaintenance_h

#include "Operation.h"

namespace Hypertable {

  /// @addtogroup Master
  /// @{

  /// %Table maintenance constants
  namespace TableMaintenance {
    /// Constant representing <i>off</i>
    const bool OFF = false;
    /// Constant representing <i>on</i>
    const bool ON = true;
  }

  /// Enables or disables maintenance for a table.
  class OperationToggleTableMaintenance : public Operation {
  public:

    /// Constructor.
    /// Initializes object by passing
    /// MetaLog::EntityType::OPERATION_TOGGLE_TABLE_MAINTENANCE to parent Operation
    /// constructor and then initializes member variables as follows:
    ///   - Sets #m_name to canonicalized <code>table_name</code>
    ///   - Adds INIT as a dependency
    /// @param context %Master context
    /// @param table_name %Table pathname
    /// @param toggle_on Flag indicating if maintenance is to be turned on or off
    OperationToggleTableMaintenance(ContextPtr &context,
                                    const std::string &table_name,
                                    bool toggle_on);

    /// Constructor with MML entity.
    /// @param context %Master context
    /// @param header %MetaLog header
    OperationToggleTableMaintenance(ContextPtr &context,
                                    const MetaLog::EntityHeader &header);
    
    /// Destructor.
    virtual ~OperationToggleTableMaintenance() { }

    /// Carries out the manual compaction operation.
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
    /// <li>Maps table name (#m_name) to table ID and stores in #m_id</li>
    /// <li>Transitions state to UPDATE_HYPERSPACE</li>
    /// <li>Persists operation to MML and drops through to next state</li>
    /// </ul></td>
    /// </tr>
    /// <tr>
    /// <td>UPDATE_HYPERSPACE</td>
    /// <td><ul>
    /// <li>If #m_toggle_on is <i>true</i>, deletes the "maintenance_disabled"
    ///     attribute of the table ID file in Hyperspace</li>
    /// <li>Otherwise, if #m_toggle_on is <i>false</i>, sets the
    ///     "maintenance_disabled" attribute of the table ID file in
    ///     Hyperspace</li>
    /// <li>Dependencies are set to METADATA and #m_id + " move range"</li>
    /// <li>Transitions state to SCAN_METADATA</li>
    /// <li>Persists operation to MML and returns</li>
    /// </ul></td>
    /// </tr>
    /// <tr>
    /// <td>SCAN_METADATA</td>
    /// <td><ul>
    /// <li>Obtains list of servers via call to
    ///     Utility::get_table_server_set()</li>
    /// <li>For each server in #m_completed, removes server as dependency</li>
    /// <li>For each server not in #m_completed, adds server as dependency</li>
    /// <li>Transitions state to ISSUE_REQUESTS</li>
    /// <li>Persists operation to MML and returns</li>
    /// </ul></td>
    /// </tr>
    /// <tr>
    /// <td>ISSUE_REQUESTS</td>
    /// <td><ul>
    /// <li>Issues a toggle maintenance request to all servers in #m_servers and
    ///     waits for their completion</li>
    /// <li>If any of the requests failed, the servers of the successfully
    ///     completed requests are added to #m_completed, the servers in
    ///     #m_servers are removed as dependencies, METADATA is added as a
    ///     dependency, #m_servers is cleared, state is transitioned back to
    ///     SCAN_METADATA, the operation sleeps for 5 seconds, the operation is
    ///     persisted to the MML and then the function returns</li>
    /// <li>Otherwise, if all requsts completed successfully, the operation is
    ///     completed with a call to complete_ok()</li>
    /// </ul></td>
    /// </tr>
    /// </table>
    virtual void execute();

    /// Returns name of operation ("OperationToggleTableMaintenance")
    /// @return %Operation name
    virtual const String name();

    /// Returns descriptive label for operation
    /// @return Descriptive label for operation
    virtual const String label();

    /// Writes human readable representation of object to output stream.
    /// @param os Output stream
    virtual void display_state(std::ostream &os);

    virtual uint16_t encoding_version() const;

    /// Returns serialized state length.
    /// This method returns the length of the serialized representation of the
    /// object state.
    /// @return Serialized length
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
    ///   <td>vstr</td><td>%Table ID (#m_id)</td>
    ///   </tr>
    ///   <tr>
    ///   <td>bool</td><td>Flag indicating if maintenance is to be enabled or
    ///                    disabled (#m_toggle_on)</td>
    ///   </tr>
    ///   <tr>
    ///   <td>i32</td><td>Size of #m_servers</td>
    ///   </tr>
    ///   <tr>
    ///   <td>vstr</td><td><b>Foreach server</b> in #m_servers, server name</td>
    ///   </tr>
    ///   <tr>
    ///   <td>i32</td><td>Size of #m_completed</td>
    ///   </tr>
    ///   <tr>
    ///   <td>vstr</td><td><b>Foreach server</b> in #m_completed, server name</td>
    ///   </tr>
    ///   <tr>
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

  private:

    /// %Table pathname
    std::string m_name;

    /// %Table ID
    std::string m_id;

    /// Set of range servers participating in toggle
    std::set<std::string> m_servers;

    /// Set of range servers that have completed toggle
    std::set<std::string> m_completed;

    /// Flag indicating if maintenance is to be toggled on or off
    bool m_toggle_on {};
  };

  /// @}

} // namespace Hypertable

#endif // Hypertable_Master_OperationToggleTableMaintenance_h
