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
/// Declarations for DispatchHandlerOperationToggleTableMaintenance.
/// This file contains declarations for
/// DispatchHandlerOperationToggleTableMaintenance, a class for enabling or
/// disabling maintenance for a table by calling appropriate functions on the
/// range servers holding the table's ranges and aggregating the responses.

#ifndef Hypertable_Master_DispatchHandlerOperationToggleTableMaintenance_h
#define Hypertable_Master_DispatchHandlerOperationToggleTableMaintenance_h

#include "DispatchHandlerOperation.h"

#include <AsyncComm/CommAddress.h>

namespace Hypertable {

  /// @addtogroup Master
  /// @{

  /// Dispatch handler for either enabling or disabling table maintenance by
  /// issuing requests to range servers and aggregating the responses.
  class DispatchHandlerOperationToggleTableMaintenance : public DispatchHandlerOperation {
  public:

    /// Constructor.
    /// @param context %Master context
    /// @param table %Table identifier of table to be compacted
    /// @param toggle_on Controls if maintenance is to be toggled on or off
    DispatchHandlerOperationToggleTableMaintenance(ContextPtr &context,
                                                   const TableIdentifier &table,
                                                   bool toggle_on) :
      DispatchHandlerOperation(context), m_table(table), m_toggle_on(toggle_on) { }

    /// Asynchronously issues table maintenance enable or disable request.
    /// This method asynchronously issues either a
    /// RangeServer::table_maintenance_enable() or
    /// RangeServer::table_maintenance_disable() request, depending on the
    /// value of #m_toggle_on, to the server specified by <code>location</code>.
    /// It supplies <i>self</i> as the dispatch handler.
    /// @param location Proxy name of range server at which to issue request
    virtual void start(const String &location) {
      CommAddress addr;
      addr.set_proxy(location);
      if (m_toggle_on)
        m_rsclient.table_maintenance_enable(addr, m_table, this);
      else
        m_rsclient.table_maintenance_disable(addr, m_table, this);
    }

  private:

    /// %Table identifier of table for which maintenance is to be toggled
    TableIdentifierManaged m_table;

    /// Flag indicating if maintenance is to be toggled on or off
    bool m_toggle_on;
  };

  // @}
}

#endif // Hypertable_Master_DispatchHandlerOperationToggleTableMaintenance_h
