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

#include "Common/Compat.h"
#include "ResponseCallbackCreateScanner.h"

using namespace Hypertable;

int
ResponseCallbackCreateScanner::response(short moreflag, int32_t id,
                StaticBuffer &ext, 
                int32_t skipped_rows, int32_t skipped_cells) {
  CommHeader header;
  header.initialize_from_request_header(m_event->header);
  CommBufPtr cbp(new CommBuf( header, 18, ext));
  cbp->append_i32(Error::OK);
  cbp->append_i16(moreflag);
  cbp->append_i32(id);              // scanner ID
  cbp->append_i32(skipped_rows);    // for OFFSET
  cbp->append_i32(skipped_cells);   // for CELL_OFFSET

  return m_comm->send_response(m_event->addr, cbp);
}


int
ResponseCallbackCreateScanner::response(short moreflag, int32_t id,
                boost::shared_array<uint8_t> &ext_buffer,
                uint32_t ext_len, int32_t skipped_rows, 
                int32_t skipped_cells) {
  CommHeader header;
  header.initialize_from_request_header(m_event->header);
  CommBufPtr cbp(new CommBuf( header, 18, ext_buffer, ext_len));
  cbp->append_i32(Error::OK);
  cbp->append_i16(moreflag);
  cbp->append_i32(id);              // scanner ID
  cbp->append_i32(skipped_rows);    // for OFFSET
  cbp->append_i32(skipped_cells);   // for CELL_OFFSET

  return m_comm->send_response(m_event->addr, cbp);
}

