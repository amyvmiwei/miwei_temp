/**
 * Copyright (C) 2007-2013 Hypertable, Inc.
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

#include "Common/Compat.h"

#include <limits>
#include "Common/Error.h"

#include "CommBuf.h"
#include "CommHeader.h"
#include "Protocol.h"
#include "ResponseCallback.h"

using namespace Hypertable;

int ResponseCallback::error(int error, const String &msg) {
  CommHeader header;
  header.initialize_from_request_header(m_event->header);
  CommBufPtr cbp;
  size_t max_msg_size = std::numeric_limits<int16_t>::max();
  if (msg.length() < max_msg_size)
    cbp = Protocol::create_error_message(header, error, msg.c_str());
  else {
    cbp = Protocol::create_error_message(header, error, msg.substr(0, max_msg_size).c_str());
  }
  return m_comm->send_response(m_event->addr, cbp);
}

int ResponseCallback::response_ok() {
  CommHeader header;
  header.initialize_from_request_header(m_event->header);
  CommBufPtr cbp(new CommBuf(header, 4));
  cbp->append_i32(Error::OK);
  return m_comm->send_response(m_event->addr, cbp);
}

