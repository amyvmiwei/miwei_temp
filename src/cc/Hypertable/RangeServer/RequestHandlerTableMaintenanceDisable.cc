/*
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
/// Definitions for RequestHandlerTableMaintenanceDisable.
/// This file contains type definitions for
/// RequestHandlerTableMaintenanceDisable, an ApplicationHandler class for
/// carrying out a RangeServer::table_maintenance_disable() request.

#include <Common/Compat.h>
#include "RequestHandlerTableMaintenanceDisable.h"

#include <Hypertable/RangeServer/RangeServer.h>

#include <Hypertable/Lib/Types.h>

#include <AsyncComm/ResponseCallback.h>

#include <Common/Error.h>
#include <Common/Logger.h>

using namespace Hypertable;

void RequestHandlerTableMaintenanceDisable::run() {
  ResponseCallback cb(m_comm, m_event);
  TableIdentifier table;

  try {
    const uint8_t *decode_ptr = m_event->payload;
    size_t decode_remain = m_event->payload_len;
    table.decode(&decode_ptr, &decode_remain);
    m_range_server->table_maintenance_disable(&cb, &table);
  }
  catch (Exception &e) {
    HT_ERROR_OUT << e << HT_END;
    cb.error(e.code(), e.what());
  }
}
