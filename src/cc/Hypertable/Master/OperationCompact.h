/*
 * Copyright (C) 2007-2012 Hypertable, Inc.
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
     *   - OperationState::INITIAL - If a table name was supplied, it maps it
     *     to a table identifier and then drops through to the next state.
     *   - OperationState::SCAN_METADATA - Scans the METADATA table to build
     *     the list of servers that hold the table to be altered.  If no table
     *     name was supplied, then all available servers are chosen.  The
     *     dependencies are reset to be Dependency::INIT, Dependency::METADATA,
     *     Dependency::SYSTEM, and the list of server proxy names.  The state
     *     is advanced to OperationState::ISSUE_REQUESTS and the method returns
     *     to allow the operation processor to update the dependency graph.
     *   - OperationState::ISSUE_REQUESTS - Issues a compact request to
     *     all participating servers and waits for their completion.  If there
     *     are any errors, state is reset back to OperationState::SCAN_METADATA,
     *     otherwise it completes.
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
     * <table style="font-family:monospace; ">
     *   <tr>
     *   <td>[vstring]</td>
     *   <td>- Pathname of table to compact</td>
     *   </tr>
     *   <tr>
     *   <td>[vstring]</td>
     *   <td>- Row key of range to compact</td>
     *   </tr>
     *   <tr>
     *   <td>[4-byte integer]</td>
     *   <td>- %Range types (see RangeServerProtocol::RangeType)</td>
     *   </tr>
     *   <tr>
     *   <td>[vstring]</td>
     *   <td>- %Table identifier</td>
     *   </tr>
     *   <tr>
     *   <td>[4-byte integer]</td>
     *   <td>- Size of completed set</td>
     *   </tr>
     *   <tr>
     *   <td>[variable]</td>
     *   <td>- Set of completed server names</td>
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
     * <table style="font-family:monospace; ">
     *   <tr>
     *   <td>[vstring]</td>
     *   <td>- Pathname of table to compact</td>
     *   </tr>
     *   <tr>
     *   <td>[vstring]</td>
     *   <td>- Row key of range to compact</td>
     *   </tr>
     *   <tr>
     *   <td>[4-byte integer]</td>
     *   <td>- %Range types (see RangeServerProtocol::RangeType)</td>
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
     *   - <b>dependencies</b> - Dependency::INIT, Dependency::METADATA,
     *     Dependency::SYSTEM, and Dependency::RECOVER_SERVER
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

    /// Set of range servers that have completed operation
    StringSet m_completed;
  };

  /// Smart pointer to OperationCompact
  typedef intrusive_ptr<OperationCompact> OperationCompactPtr;

  /* @}*/

} // namespace Hypertable

#endif // HYPERTABLE_OPERATIONCOMPACT_H
