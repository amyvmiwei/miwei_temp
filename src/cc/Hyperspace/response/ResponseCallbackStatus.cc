/*
 * Copyright (C) 2007-2012 Hypertable, Inc.
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
#include "Common/Error.h"
#include "Common/Serialization.h"

#include "AsyncComm/CommBuf.h"

#include "ResponseCallbackStatus.h"

using namespace Hyperspace;
using namespace Hypertable;

int ResponseCallbackStatus::response(int32_t code, const std::string &output) {
  CommHeader header;
  header.initialize_from_request_header(m_event->header);
  CommBufPtr cbp(new CommBuf(header, 8 + Serialization::encoded_length_vstr(output)));
  cbp->append_i32(Error::OK);
  cbp->append_i32(code);
  cbp->append_vstr(output);
  return m_comm->send_response(m_event->addr, cbp);
}
