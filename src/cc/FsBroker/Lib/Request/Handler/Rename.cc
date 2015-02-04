/*
 * Copyright (C) 2007-2015 Hypertable, Inc.
 *
 * This file is part of Hypertable.
 *
 * Hypertable is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 3
 * of the License, or any later version.
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
/// Definitions for Rename request handler.
/// This file contains definitions for Rename, a server-side request handler
/// used to invoke the <i>rename</i> function of a file system broker.

#include <Common/Compat.h>

#include "Rename.h"

#include <FsBroker/Lib/Request/Parameters/Rename.h>

#include <AsyncComm/ResponseCallback.h>

#include <Common/Error.h>
#include <Common/Logger.h>
#include <Common/Serialization.h>

using namespace Hypertable;
using namespace Hypertable::FsBroker::Lib;
using namespace Hypertable::FsBroker::Lib::Request::Handler;

void Rename::run() {
  ResponseCallback cb(m_comm, m_event);
  const uint8_t *ptr = m_event->payload;
  size_t remain = m_event->payload_len;

  try {
    Request::Parameters::Rename params;
    params.decode(&ptr, &remain);
    m_broker->rename(&cb, params.get_from(), params.get_to());
  }
  catch (Exception &e) {
    HT_ERROR_OUT << e << HT_END;
    cb.error(e.code(), "Error handling rename request");
  }
}
