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
#include "AsyncComm/Protocol.h"

#include "Common/Error.h"
#include "Common/Logger.h"

#include "TableMutatorAsyncDispatchHandler.h"
#include "TableMutatorAsyncHandler.h"

using namespace Hypertable;
using namespace Serialization;

/**
 *
 */
TableMutatorAsyncDispatchHandler::TableMutatorAsyncDispatchHandler(
    ApplicationQueueInterfacePtr &app_queue, TableMutatorAsync *mutator,
    uint32_t scatter_buffer, TableMutatorAsyncSendBuffer *send_buffer, bool auto_refresh)
  : m_app_queue(app_queue), m_mutator(mutator),
    m_scatter_buffer(scatter_buffer), m_send_buffer(send_buffer),
    m_auto_refresh(auto_refresh) {
}

/**
 *
 */
void TableMutatorAsyncDispatchHandler::handle(EventPtr &event_ptr) {
  int32_t error;

  if (event_ptr->type == Event::MESSAGE) {
    error = Protocol::response_code(event_ptr);
    if (error != Error::OK) {
      if (m_auto_refresh &&
          (error == Error::RANGESERVER_GENERATION_MISMATCH ||
           error == Error::RANGESERVER_TABLE_NOT_FOUND))
        m_send_buffer->add_retries_all(true, error);
      else
        m_send_buffer->add_errors_all(error);
    }
    else {
      const uint8_t *decode_ptr = event_ptr->payload + 4;
      size_t decode_remain = event_ptr->payload_len - 4;
      uint32_t count, offset, len;

      if (decode_remain == 0) {
        m_send_buffer->clear();
      }
      else {
        while (decode_remain) {
          try {
            error = decode_i32(&decode_ptr, &decode_remain);
            count = decode_i32(&decode_ptr, &decode_remain);
            offset = decode_i32(&decode_ptr, &decode_remain);
            len = decode_i32(&decode_ptr, &decode_remain);
          }
          catch (Exception &e) {
            HT_ERROR_OUT << e << HT_END;
            break;
          }
          if (error == Error::RANGESERVER_OUT_OF_RANGE ||
              error == Error::RANGESERVER_RANGE_NOT_YET_ACKNOWLEDGED)
            m_send_buffer->add_retries(count, offset, len);
          else {
            m_send_buffer->add_errors(error, count, offset, len);
          }
        }
      }
    }
  }
  else if (event_ptr->type == Event::ERROR) {
    m_send_buffer->add_retries_all();
    HT_WARNF("%s, will retry ...", event_ptr->to_str().c_str());
  }
  else {
    // this should never happen
    HT_ERRORF("%s", event_ptr->to_str().c_str());
  }

  bool complete = m_send_buffer->counterp->decrement();
  if (complete) {
    TableMutatorAsyncHandler *handler = new TableMutatorAsyncHandler(m_mutator, m_scatter_buffer);
    m_app_queue->add(handler);
  }
}

