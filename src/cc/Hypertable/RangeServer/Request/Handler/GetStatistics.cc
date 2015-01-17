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

/// @file
/// Definitions for GetStatistics.
/// This file contains the definition for GetStatistics, a
/// class for de-marshalling request parameters and issuing a
/// RangeServer::get_statistics() request.

#include <Common/Compat.h>

#include "GetStatistics.h"

#include <Hypertable/RangeServer/Response/Callback/GetStatistics.h>
#include <Hypertable/RangeServer/RangeServer.h>

#include <Hypertable/Lib/RangeServer/Request/Parameters/GetStatistics.h>

#include <AsyncComm/ResponseCallback.h>

using namespace Hypertable;
using namespace Hypertable::RangeServer;
using namespace Hypertable::RangeServer::Request::Handler;

void GetStatistics::run() {
  Response::Callback::GetStatistics cb(m_comm, m_event);

  try {
    const uint8_t *ptr = m_event->payload;
    size_t remain = m_event->payload_len;
    Lib::RangeServer::Request::Parameters::GetStatistics params;
    params.decode(&ptr, &remain);
    m_range_server->get_statistics(&cb, params.system_variables(),
                                   params.generation());
  }
  catch (Exception &e) {
    HT_ERROR_OUT << e << HT_END;
    cb.error(e.code(), "Error handling GetStatistics message");
  }
}
