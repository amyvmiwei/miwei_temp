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

#include <Common/Compat.h>
#include "ResponseCallbackReaddir.h"

#include <Common/Error.h>
#include <Common/Serialization.h>

#include <AsyncComm/CommBuf.h>

using namespace Hypertable;
using namespace FsBroker;
using namespace Serialization;

int ResponseCallbackReaddir::response(std::vector<Filesystem::Dirent> &listing) {
  CommHeader header;
  header.initialize_from_request_header(m_event->header);
  uint32_t len = 8;
  foreach_ht (const Filesystem::Dirent &entry, listing)
    len += entry.encoded_length();
  CommBufPtr cbp( new CommBuf(header, len) );
  cbp->append_i32(Error::OK);
  cbp->append_i32(listing.size());
  foreach_ht (const Filesystem::Dirent &entry, listing)
    entry.encode(cbp->get_data_ptr_address());
  return m_comm->send_response(m_event->addr, cbp);
}
