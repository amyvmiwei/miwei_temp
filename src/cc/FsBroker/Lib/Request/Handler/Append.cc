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
/// Definitions for Append request handler.
/// This file contains definitions for Append, a server-side request handler
/// used to invoke the <i>append</i> function of a file system broker.

#include <Common/Compat.h>

#include "Append.h"

#include <FsBroker/Lib/Response/Callback/Append.h>
#include <FsBroker/Lib/Request/Parameters/Append.h>

#include <AsyncComm/CommHeader.h>

#include <Common/Error.h>
#include <Common/Filesystem.h>
#include <Common/Logger.h>
#include <Common/Serialization.h>

using namespace Hypertable;
using namespace Hypertable::FsBroker::Lib;
using namespace Hypertable::FsBroker::Lib::Request::Handler;

void Append::run() {
  Response::Callback::Append cb(m_comm, m_event);
  const uint8_t *ptr = m_event->payload;
  size_t remain = m_event->payload_len;

  try {
    Request::Parameters::Append params;

    params.decode(&ptr, &remain);

    if (m_event->header.alignment) {
      size_t padding = m_event->header.alignment - (ptr - m_event->payload);
      remain -= padding;
      ptr += padding;
    }

    if (remain < params.get_size())
      HT_THROW_INPUT_OVERRUN(remain, params.get_size());

    m_broker->append(&cb, params.get_fd(), params.get_size(),
		     ptr, static_cast<Filesystem::Flags>(params.get_flags()));

  }
  catch (Exception &e) {
    HT_ERROR_OUT << e << HT_END;
    cb.error(e.code(), "Error handling APPEND message");
  }
}
