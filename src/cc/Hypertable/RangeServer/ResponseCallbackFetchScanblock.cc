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

#include <Common/Compat.h>

#include "ResponseCallbackFetchScanblock.h"

#include <AsyncComm/CommBuf.h>

#include <Common/Error.h>

using namespace Hypertable;

int
ResponseCallbackFetchScanblock::response(short moreflag, int32_t id,
                                         StaticBuffer &ext,
                                         ProfileDataScanner &profile_data) {
  CommHeader header;
  header.initialize_from_request_header(m_event->header);
  size_t profile_data_len =
    (m_event->header.flags & CommHeader::FLAGS_BIT_PROFILE) ?
    profile_data.encoded_length() : 0;
  CommBufPtr cbp(new CommBuf( header, 18+profile_data_len, ext));
  cbp->append_i32(Error::OK);
  cbp->append_i16(moreflag);
  cbp->append_i32(id);              // scanner ID
  cbp->append_i32(0);               // skipped_rows
  cbp->append_i32(0);               // skipped_cells

  if (profile_data_len)
    profile_data.encode(cbp->get_data_ptr_address());

  return m_comm->send_response(m_event->addr, cbp);
}

