/* -*- c++ -*-
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

#include "CreateScanner.h"

#include <Hypertable/RangeServer/QueryCache.h>
#include <Hypertable/RangeServer/RangeServer.h>

#include <Hypertable/Lib/RangeServer/Request/Parameters/CreateScanner.h>

#include <AsyncComm/ResponseCallback.h>

#include <Common/Error.h>
#include <Common/Logger.h>
#include <Common/Serialization.h>
#include <Common/md5.h>

using namespace Hypertable;
using namespace Hypertable::RangeServer::Request::Handler;

void CreateScanner::run() {
  Response::Callback::CreateScanner cb(m_comm, m_event);

  try {
    const uint8_t *ptr = m_event->payload;
    size_t remain = m_event->payload_len;
    QueryCache::Key key;
    Lib::RangeServer::Request::Parameters::CreateScanner params;

    const uint8_t *base = ptr;
    params.decode(&ptr, &remain);

    if (params.scan_spec().cacheable()) {
      md5_csum((unsigned char *)base, ptr-base,
               reinterpret_cast<unsigned char *>(key.digest));
      m_range_server->create_scanner(&cb, params.table(), params.range_spec(),
                                     params.scan_spec(), &key);
    }
    else
      m_range_server->create_scanner(&cb, params.table(), params.range_spec(),
                                     params.scan_spec(), 0);
  }
  catch (Exception &e) {
    HT_ERROR_OUT << e << HT_END;
    cb.error(e.code(), e.what());
  }
}
