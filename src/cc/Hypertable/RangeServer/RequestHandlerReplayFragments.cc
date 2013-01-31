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
#include "Common/Error.h"
#include "Common/Logger.h"

#include "AsyncComm/ResponseCallback.h"
#include "Common/Serialization.h"

#include "Hypertable/Lib/Types.h"
#include "Hypertable/Lib/RangeRecoveryReceiverPlan.h"

#include "RangeServer.h"
#include "RequestHandlerReplayFragments.h"

using namespace Hypertable;
using namespace Serialization;

/**
 *
 */
void RequestHandlerReplayFragments::run() {
  ResponseCallback cb(m_comm, m_event);
  int64_t op_id;
  int type;
  int plan_generation;
  uint32_t replay_timeout;
  String recover_location;
  vector<uint32_t> fragments;
  RangeRecoveryReceiverPlan receiver_plan;
  uint32_t nn;

  const uint8_t *decode_ptr = m_event->payload;
  size_t decode_remain = m_event->payload_len;

  try {
    op_id = decode_i64(&decode_ptr, &decode_remain);
    recover_location = decode_vstr(&decode_ptr, &decode_remain);
    plan_generation = decode_i32(&decode_ptr, &decode_remain);
    type = decode_i32(&decode_ptr, &decode_remain);
    nn = decode_i32(&decode_ptr, &decode_remain);
    for (uint32_t ii = 0; ii < nn; ++ii)
      fragments.push_back(decode_i32(&decode_ptr, &decode_remain));
    receiver_plan.decode(&decode_ptr, &decode_remain);
    replay_timeout = decode_i32(&decode_ptr, &decode_remain);

  }
  catch (Exception &e) {
    HT_ERROR_OUT << e << HT_END;
    cb.error(e.code(), e.what());
    return;
  }

  try {
    m_range_server->replay_fragments(&cb, op_id, recover_location,
        plan_generation, type, fragments, receiver_plan, replay_timeout);
  }
  catch (Exception &e) {
    HT_ERROR_OUT << e << HT_END;
  }
}
