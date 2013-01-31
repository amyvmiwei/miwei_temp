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

#include "Master.h"
#include "RequestHandlerAttrSet.h"

using namespace Hyperspace;
using namespace Hypertable;
using namespace Serialization;

/**
 *
 */
void RequestHandlerAttrSet::run() {
  ResponseCallback cb(m_comm, m_event);
  size_t decode_remain = m_event->payload_len;
  const uint8_t *decode_ptr = m_event->payload;

  try {
    bool has_name = decode_bool(&decode_ptr, &decode_remain);
    uint64_t handle = 0;
    const char* name = 0;
    uint32_t oflags = 0;
    if (has_name) {
      name = decode_vstr(&decode_ptr, &decode_remain);
      oflags = decode_i32(&decode_ptr, &decode_remain);
    }
    else
      handle = decode_i64(&decode_ptr, &decode_remain);

    Attribute attr;
    std::vector<Attribute> attrs;
    uint32_t attr_count = decode_i32(&decode_ptr, &decode_remain);
    attrs.reserve(attr_count);

    while (attr_count--) {
      attr.name = decode_vstr(&decode_ptr, &decode_remain);
      attr.value = decode_vstr(&decode_ptr, &decode_remain, &attr.value_len);
      attrs.push_back(attr);
    }

    m_master->attr_set(&cb, m_session_id, handle, name, oflags, attrs);
  }
  catch (Exception &e) {
    HT_ERROR_OUT << e << HT_END;
    cb.error(e.code(), "Error handling ATTRSET message");
  }
}
