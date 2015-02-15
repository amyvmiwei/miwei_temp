/*
 * Copyright (C) 2007-2015 Hypertable, Inc.
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

#include "DispatchHandlerOperationSystemStatus.h"

#include <Hypertable/Lib/RangeServer/Response/Parameters/Status.h>

#include <Common/Error.h>
#include <Common/Logger.h>
#include <Common/Time.h>

using namespace Hypertable;

DispatchHandlerOperationSystemStatus::DispatchHandlerOperationSystemStatus(ContextPtr &context, Timer &timer) :
  DispatchHandlerOperation(context), m_timer(timer) {
}

void DispatchHandlerOperationSystemStatus::initialize(std::vector<Result> &results) {
  for (auto &result : results)
    m_index[result.addr] = &result;
  m_timer.start();
}


void DispatchHandlerOperationSystemStatus::start(const String &location) {
  CommAddress addr;
  addr.set_proxy(location);
  m_rsclient.status(addr, this, m_timer);
}


void DispatchHandlerOperationSystemStatus::result_callback(const EventPtr &event) {
  int error;
  auto iter = m_index.find(event->addr);
  if (iter != m_index.end()) {
    Result *result = iter->second;
    if (event->type == Event::MESSAGE) {
      if ((error = Protocol::response_code(event)) != Error::OK) {
        result->error = error;
        result->message = Protocol::string_format_message(event);
      }
      else {
        result->error = Error::OK;
        try {
          size_t remaining = event->payload_len - 4;
          const uint8_t *ptr = event->payload + 4;
          Lib::RangeServer::Response::Parameters::Status params;
          params.decode(&ptr, &remaining);
          result->status = params.status();
        }
        catch (Exception &e) {
          result->error = e.code();
          result->message = e.what();
        }
      }
    }
    else {
      result->error = event->error;
    }
  }
  else
    HT_WARN_OUT << "Received 'status' response from unexpected connection " 
                << event->addr.format() << HT_END;
}
