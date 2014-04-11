/**
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

#include "AsyncComm/CommBuf.h"
#include "Common/Serialization.h"

#include "ResponseCallbackPosixReaddir.h"

using namespace Hypertable;
using namespace FsBroker;
using namespace Serialization;

int
ResponseCallbackPosixReaddir::response(std::vector<Filesystem::DirectoryEntry> &listing) {
  CommHeader header;
  header.initialize_from_request_header(m_event->header);

  uint32_t len = 8;
  for (size_t i = 0; i < listing.size(); i++)
    len += encoded_length_str16(listing[i].name) + 4 + 4;

  CommBufPtr cbp(new CommBuf(header, len));
  cbp->append_i32(Error::OK);
  cbp->append_i32(listing.size());
  for (size_t i = 0; i < listing.size(); i++) {
    cbp->append_str16(listing[i].name);
    cbp->append_i32(listing[i].flags);
    cbp->append_i32(listing[i].length);
  }
  return m_comm->send_response(m_event->addr, cbp);
}
