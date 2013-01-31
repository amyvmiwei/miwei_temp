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
#include "Common/Logger.h"

#include "AsyncComm/ResponseCallback.h"
#include "Common/Serialization.h"

#include "Hypertable/Lib/Types.h"

#include "Master.h"
#include "RequestHandlerMkdir.h"

using namespace Hyperspace;
using namespace Hypertable;
using namespace Serialization;

/**
 *
 */
void RequestHandlerMkdir::run() {
  ResponseCallback cb(m_comm, m_event);
  size_t decode_remain = m_event->payload_len;
  const uint8_t *decode_ptr = m_event->payload;

  try {
    const char *name = decode_vstr(&decode_ptr, &decode_remain);
    bool create_intermediate = decode_bool(&decode_ptr, &decode_remain);

    Attribute attr;
    std::vector<Attribute> init_attrs;
    uint32_t attr_count = decode_i32(&decode_ptr, &decode_remain);
    init_attrs.reserve(attr_count);

    while (attr_count--) {
      attr.name = decode_vstr(&decode_ptr, &decode_remain);
      attr.value = decode_vstr(&decode_ptr, &decode_remain, &attr.value_len);
      init_attrs.push_back(attr);
    }

    if (create_intermediate)
      m_master->mkdirs(&cb, m_session_id, name, init_attrs);
    else
      m_master->mkdir(&cb, m_session_id, name, init_attrs);
  }
  catch (Exception &e) {
    HT_ERROR_OUT << e << HT_END;
    cb.error(e.code(), "Encoding problem with MKDIR message");
  }
}
