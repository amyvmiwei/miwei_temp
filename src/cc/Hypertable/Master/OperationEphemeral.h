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
 * Declarations for OperationEphemeral.
 * This file contains declarations for OperationEphemeral, an abstract base
 * class that that represents a master operation that does not get logged
 * to the MML.
 */

#ifndef HYPERTABLE_OPERATION_EPHEMERAL_H
#define HYPERTABLE_OPERATION_EPHEMERAL_H

#include <ctime>
#include <set>

#include "AsyncComm/Event.h"

#include "Common/Mutex.h"
#include "Common/ScopeGuard.h"
#include "Common/Time.h"

#include "Hypertable/Lib/MetaLogEntity.h"

#include "Context.h"
#include "Operation.h"

namespace Hypertable {

  /** @addtogroup Master
   *  @{
   */

  /** Abstract base class for ephemeral operations.
   * This class is used as the base class for operations that do not persist
   * themselves to the MML.  It provides implementations for the encoding and
   * decoding operations which simply assert to assure that they're not called.
   */
  class OperationEphemeral : public Operation {
  public:

    /** Constructor with operation type specifier.
     * Initializes object by passing parameters to parent class constructor and then
     * sets #m_ephemeral to <i>true</i>.
     * @param context %Master context
     * @param type %Operation type
     */
    OperationEphemeral(ContextPtr &context, int32_t type) : 
      Operation(context, type) {
      m_ephemeral = true;
    }

    /** Constructor with request Event and operation type specifier.
     * Initializes object by passing parameters to parent class constructor and then
     * sets #m_ephemeral to <i>true</i>.
     * @param context %Master context
     * @param event Event object
     * @param type %Operation type
     */
    OperationEphemeral(ContextPtr &context, EventPtr &event, int32_t type) :
      Operation(context, event, type) {
      m_ephemeral = true;
    }

    /** Constructor with MetaLog::EntityHeader.
     * Initializes object by passing parameters to parent class constructor and then
     * sets #m_ephemeral to <i>true</i>.
     * @param context %Master context
     * @param header_ MetaLog header
     */
    OperationEphemeral(ContextPtr &context,
                       const MetaLog::EntityHeader &header_) :
      Operation(context, header_) {
      m_ephemeral = true;
    }

    /** Destructor */
    virtual ~OperationEphemeral() { }

    /** Encoded length of operation state.
     * @note This method unconditionally asserts because it should not be called by
     * ephemeral operations.
     */
    virtual size_t encoded_state_length() const {
      HT_ASSERT(!"Ephemeral operation");
    }

    /** Encode operation state.
     * @note This method unconditionally asserts because it should not be called by
     * ephemeral operations.
     */
    virtual void encode_state(uint8_t **bufp) const {
      HT_ASSERT(!"Ephemeral operation");
    }

    /** Decode operation state.
     * @note This method unconditionally asserts because it should not be called by
     * ephemeral operations.
     */
    virtual void decode_state(const uint8_t **bufp, size_t *remainp) {
      HT_ASSERT(!"Ephemeral operation");
    }

    /** Returns version of encoding format.
     * @note This method unconditionally asserts because it should not be called by
     * ephemeral operations.
     */
    virtual uint16_t encoding_version() const {
      HT_ASSERT(!"Ephemeral operation");
    }

    /** Length of encoded operation.
     * @note This method unconditionally asserts because it should not be called by
     * ephemeral operations.
     */
    virtual size_t encoded_length() const {
      HT_ASSERT(!"Ephemeral operation");
    }

    /** Encode operation.
     * @note This method unconditionally asserts because it should not be called by
     * ephemeral operations.
     */
    virtual void encode(uint8_t **bufp) const {
      HT_ASSERT(!"Ephemeral operation");
    }

    /** Decode operation.
     * @note This method unconditionally asserts because it should not be called by
     * ephemeral operations.
     */
    virtual void decode(const uint8_t **bufp, size_t *remainp,
                        uint16_t definition_version) {
      HT_ASSERT(!"Ephemeral operation");
    }

  };

  /** @} */

} // namespace Hypertable

#endif // HYPERTABLE_OPERATION_EPHEMERAL_H
