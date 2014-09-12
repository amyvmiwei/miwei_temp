/** -*- c++ -*-
 * Copyright (C) 2007-2012 Hypertable, Inc.
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

#include "ResponseCallbackCreateScanner.h"

#include <AsyncComm/CommBuf.h>

#include <Common/Error.h>

using namespace Hypertable;

int
ResponseCallbackCreateScanner::response(short moreflag, int32_t id,
                                        StaticBuffer &ext, int32_t skipped_rows,
                                        int32_t skipped_cells,
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
  cbp->append_i32(skipped_rows);    // for OFFSET
  cbp->append_i32(skipped_cells);   // for CELL_OFFSET

  if (profile_data_len)
    profile_data.encode(cbp->get_data_ptr_address());

  return m_comm->send_response(m_event->addr, cbp);
}


int
ResponseCallbackCreateScanner::response(short moreflag, int32_t id,
                                       boost::shared_array<uint8_t> &ext_buffer,
                                        uint32_t ext_len, int32_t skipped_rows, 
                                        int32_t skipped_cells,
                                        ProfileDataScanner &profile_data) {
  CommHeader header;
  header.initialize_from_request_header(m_event->header);
  size_t profile_data_len =
    (m_event->header.flags & CommHeader::FLAGS_BIT_PROFILE) ?
    profile_data.encoded_length() : 0;
  CommBufPtr cbp(new CommBuf( header, 18+profile_data_len, ext_buffer, ext_len));
  cbp->append_i32(Error::OK);
  cbp->append_i16(moreflag);
  cbp->append_i32(id);              // scanner ID
  cbp->append_i32(skipped_rows);    // for OFFSET
  cbp->append_i32(skipped_cells);   // for CELL_OFFSET

  if (profile_data_len)
    profile_data.encode(cbp->get_data_ptr_address());

  return m_comm->send_response(m_event->addr, cbp);
}

