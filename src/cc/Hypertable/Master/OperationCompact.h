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
 * Declarations for OperationCompact.
 * This file contains declarations for OperationCompact, an Operation class
 * for carrying out a manual compaction operation.
 */

#ifndef HYPERTABLE_OPERATIONCOMPACT_H
#define HYPERTABLE_OPERATIONCOMPACT_H

#include "Common/StringExt.h"

#include "Operation.h"

namespace Hypertable {

  /** @addtogroup Master
   *  @{
   */

  /** Carries out a manual compaction operation. */
  class OperationCompact : public Operation {
  public:

    /** Constructor for constructing object from %MetaLog entry.
     * @param context %Master context
     * @param header_ %MetaLog header
     */
    OperationCompact(ContextPtr &context, const MetaLog::EntityHeader &header_);
    
    /** Constructor for constructing object from AsyncComm event 
     * @param context %Master context
     * @param event %Event received from AsyncComm from client request
     */
    OperationCompact(ContextPtr &context, EventPtr &event);

    /** Destructor. */
    virtual ~OperationCompact() { }

    /** Carries out the manual compaction operation.
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
     * <li>If a table name was supplied, it maps it to a table identifier (#m_id)</li>
     * <li>If supplied table name not found in name map, completes with
     * Error::TABLE_NOT_FOUND</li>
     * <li>Otherwise, sets dependencies to Dependency::METADATA and transitions
     * to SCAN_METADATA</li>
     * </ul></td>
     * </tr>
     * <tr>
     * <td>SCAN_METADATA</td>
     * <td><ul>
     * <li>Scans the METADATA table and populates #m_servers to hold the set
     * of servers that hold the table to be altered which are not in the
     * #m_completed set.  If no table name was supplied, then #m_servers is
     * set to all available servers which are not in the #m_completed set</li>
     * <li>Dependencies are set to server names in #m_servers</li>
     * <li>Transitions to the ISSUE_REQUESTS state</li>
     * <li>Persists operation to MML and returns</li>
     * </ul></td>
     * </tr>
     * <tr>
     * <td>ISSUE_REQUESTS</td>
     * <td><ul>
     * <li>Issues a compact request to all servers in #m_servers and waits
     * for their completion</li>
     * <li>If there are any errors, for each server that was successful or
     * returned with Error::TABLE_NOT_FOUND,
     * the server name is added to #m_completed.  Dependencies are then set back
     * to just Dependency::METADATA, the state is reset back to SCAN_METADATA,
     * the operation is persisted to the MML, and the method returns</li>
     * <li>Otherwise state is transitioned to COMPLETED</li>
     * </ul></td>
     * </tr>
     * </table>
     */
    virtual void execute();

    /** Returns name of operation ("OperationCompact")
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
     *   <td>vstr</td><td>Pathname of table to compact</td>
     *   </tr>
     *   <tr>
     *   <td>vstr</td><td>Row key of range to compact</td>
     *   </tr>
     *   <tr>
     *   <td>i32</td><td>%Range types (see RangeServerProtocol::RangeType)</td>
     *   </tr>
     *   <tr>
     *   <td>vstr</td><td>%Table identifier</td>
     *   </tr>
     *   <tr>
     *   <td>i32</td><td>Size of #m_completed</td>
     *   </tr>
     *   <tr>
     *   <td>vstr</td><td><b>Foreach server</b> in #m_completed, server name</td>
     *   </tr>
     *   <tr>
     *   <td>i32</td><td>[VERSION 2] Size of #m_servers</td>
     *   </tr>
     *   <tr>
     *   <td>vstr</td><td>[VERSION 2] <b>Foreach server</b> in #m_servers, server name</td>
     *   </tr>
     * </table>
     * @param bufp Address of destination buffer pointer (advanced by call)
     */
    virtual void encode_state(uint8_t **bufp) const;

    /** Reads serialized encoding of object state.
     * This method restores the state of the object by decoding a serialized
     * representation of the state from the memory location pointed to by
     * <code>*bufp</code>.  See encode() for a description of the
     * serialized format.
     * @param bufp Address of source buffer pointer (advanced by call)
     * @param remainp Amount of remaining buffer pointed to by <code>*bufp</code>
     * (decremented by call)
     */
    virtual void decode_state(const uint8_t **bufp, size_t *remainp);

    /** Decodes a request that triggered the operation.
     * This method decodes a request sent from a client that caused this
     * object to get created.  The encoding has the following format:
     * <table>
     *   <tr>
     *   <th>Encoding</th><th>Description</th>
     *   </tr>
     *   <tr>
     *   <td>vstr</td><td> Pathname of table to compact</td>
     *   </tr>
     *   <tr>
     *   <td>vstr</td><td> Row key of range to compact</td>
     *   </tr>
     *   <tr>
     *   <td>i32</td><td> %Range types (see RangeServerProtocol::RangeType)</td>
     *   </tr>
     * </table>
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

    /// Pathname of table to compact
    String m_name;

    /// Row key of range to compact
    String m_row;

    /// %Range type specification (see RangeServerProtocol::RangeType)
    uint32_t m_range_types;

    /// %Table identifier
    String m_id;

    /// Set of participating range servers
    StringSet m_servers;

    /// Set of range servers that have completed operation
    StringSet m_completed;
  };

  /* @}*/

} // namespace Hypertable

#endif // HYPERTABLE_OPERATIONCOMPACT_H
