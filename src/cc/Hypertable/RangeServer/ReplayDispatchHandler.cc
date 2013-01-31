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
#include "ReplayDispatchHandler.h"

using namespace std;
using namespace Hypertable;

void ReplayDispatchHandler::handle(Hypertable::EventPtr &event) {
  ScopedLock lock(m_mutex);
  int32_t error;
  QualifiedRangeSpec range;
  String error_msg;
  String msg;

  if (event->type == Event::MESSAGE) {
    error = Protocol::response_code(event);
    const uint8_t *decode_ptr = event->payload + 4;
    size_t decode_remain = event->payload_len - 4;

    if (error != Error::OK) {
      error_msg = Serialization::decode_str16(&decode_ptr, &decode_remain);
      range.decode(&decode_ptr, &decode_remain);
      if (error == Error::RANGESERVER_FRAGMENT_ALREADY_PROCESSED) {
        HT_INFOF("%s - %s", Error::get_text(error), error_msg.c_str());
        // Currently, this block should never be executed
      }
      else {
        m_error_msg = format("Replay to %s failed - %s",
                             event->proxy ? event->proxy : event->addr.format().c_str(),
                             error_msg.c_str());
        HT_INFO_OUT << m_error_msg << HT_END;
        m_error = error;
      }
    }
  }
  else {
    m_error_msg = format("Replay to %s failed",
                         event->proxy ? event->proxy : event->addr.format().c_str());
    HT_INFO_OUT << m_error_msg << HT_END;
    m_error = event->error;
  }

  HT_ASSERT(m_outstanding>0);
  m_outstanding--;
  if (m_outstanding == 0)
    m_cond.notify_all();
}

void ReplayDispatchHandler::add(const CommAddress &addr,
        const QualifiedRangeSpec &range, uint32_t fragment,
        StaticBuffer &buffer) {
  {
    ScopedLock lock(m_mutex);
    m_outstanding++;
  }

  try {
    m_rsclient.phantom_update(addr, m_recover_location, m_plan_generation, 
                              range, fragment, buffer, this);
  }
  catch (Exception &e) {
    ScopedLock lock(m_mutex);
    HT_ERROR_OUT << "Error sending phantom updates for range " << range
        << " to " << addr.to_str() << "-" << e << HT_END;
    m_outstanding--;
    HT_ASSERT(addr.is_proxy());
    m_error_msg = e.what();
    m_error = e.code();
  }
}

void ReplayDispatchHandler::wait_for_completion() {
  ScopedLock lock(m_mutex);
  while (m_outstanding)
    m_cond.wait(lock);
  if (m_error != Error::OK)
    HT_THROW(m_error, m_error_msg);
}

