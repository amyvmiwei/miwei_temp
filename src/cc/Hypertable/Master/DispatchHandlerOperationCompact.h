/*
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

/** @file
 * Declarations for DispatchHandlerOperationCompact.
 * This file contains declarations for DispatchHandlerOperationCompact, a
 * class for issuing RangeServer::compact commands to a set of range
 * servers and aggregating the responses.
 */

#ifndef HYPERTABLE_DISPATCHHANDLEROPERATIONCOMPACT_H
#define HYPERTABLE_DISPATCHHANDLEROPERATIONCOMPACT_H

#include "AsyncComm/CommAddress.h"

#include "DispatchHandlerOperation.h"

namespace Hypertable {

  /** @addtogroup Master
   *  @{
   */

  /** Dispatch handler to aggregate a set of RangeServer::compact results.
   */
  class DispatchHandlerOperationCompact : public DispatchHandlerOperation {
  public:

    /** Constructor.
     * @param context %Master context
     * @param table %Table identifier of table to be compacted
     * @param row Row key of range to compact
     * @param range_types %Range type specification
     * (see RangeServerProtocol::RangeType)
     */
    DispatchHandlerOperationCompact(ContextPtr &context, const TableIdentifier &table,
                                    const String &row, uint32_t range_types) :
      DispatchHandlerOperation(context), m_table(table),
      m_row(row), m_range_types(range_types) { }

    /** Asynchronously issues compact request.
     * This method asynchronously issues a RangeServer::compact request to the
     * server specified by <code>location</code>.  It supplies <i>self</i> as
     * the dispatch handler.
     * @param location Proxy name of range server at which to issue request
     */
    virtual void start(const String &location) {
      CommAddress addr;
      addr.set_proxy(location);
      m_rsclient.compact(addr, m_table, m_row, m_range_types, this);
    }

  private:

    /// %Table identifier of table to be compacted
    TableIdentifierManaged m_table;

    /// Row key of range to compact
    String m_row;

    /// %Range type specification (see RangeServerProtocol::RangeType)
    uint32_t m_range_types;
  };

  /// Smart pointer to DispatchHandlerOperationCompact
  typedef std::shared_ptr<DispatchHandlerOperationCompact> DispatchHandlerOperationCompactPtr;

  /* @}*/
}

#endif // HYPERTABLE_DISPATCHHANDLEROPERATIONCOMPACT_H
