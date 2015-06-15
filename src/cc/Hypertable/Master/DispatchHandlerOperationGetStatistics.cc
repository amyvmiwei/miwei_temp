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

#include "DispatchHandlerOperationGetStatistics.h"

#include <Hypertable/Lib/RangeServer/Response/Parameters/GetStatistics.h>
#include <Hypertable/Lib/StatsRangeServer.h>

#include <Common/Error.h>
#include <Common/Logger.h>
#include <Common/Time.h>

using namespace Hypertable;
using namespace std;

DispatchHandlerOperationGetStatistics::DispatchHandlerOperationGetStatistics(ContextPtr &context) :
  DispatchHandlerOperation(context), m_timer(context->props->get_i32("Hypertable.Monitoring.Interval")) {
}

void DispatchHandlerOperationGetStatistics::initialize(std::vector<RangeServerStatistics> &results) {
  int64_t now = get_ts64();
  for (size_t i = 0; i<results.size(); i++) {
    m_index[results[i].addr] = &results[i];
    results[i].fetch_error = Error::NO_RESPONSE;
    results[i].fetch_timestamp = now;
    results[i].stats = make_shared<StatsRangeServer>();
  }
  m_context->system_state->get(m_specs, &m_generation);
  m_timer.start();
}


void DispatchHandlerOperationGetStatistics::start(const String &location) {
  CommAddress addr;
  addr.set_proxy(location);
  m_rsclient.get_statistics(addr, m_specs, m_generation, this, m_timer);
}


void DispatchHandlerOperationGetStatistics::result_callback(const EventPtr &event) {
  int error;
  int64_t now = get_ts64();
  SockAddrMap<RangeServerStatistics *>::iterator iter = m_index.find(event->addr);
  if (iter != m_index.end()) {
    RangeServerStatistics *stats = iter->second;
    stats->fetch_duration = now - stats->fetch_timestamp;
    if (event->type == Event::MESSAGE) {
      if ((error = Protocol::response_code(event)) != Error::OK) {
        stats->fetch_error = error;
        stats->fetch_error_msg = Protocol::string_format_message(event);
      }
      else {
        stats->fetch_error = 0;
        stats->fetch_error_msg = "";
        try {
          size_t remaining = event->payload_len - 4;
          const uint8_t *ptr = event->payload + 4;
          Lib::RangeServer::Response::Parameters::GetStatistics params;
          params.decode(&ptr, &remaining);
          *stats->stats.get() = params.stats();
          stats->stats_timestamp = stats->fetch_timestamp;
        }
        catch (Exception &e) {
          stats->fetch_error = e.code();
          stats->fetch_error_msg = e.what();
        }
      }
    }
    else {
      stats->fetch_error = event->error;
      stats->fetch_error_msg = "";
    }
  }
  else
    HT_ERROR_OUT << "Received 'get_statistics' response from unexpected connection " 
                 << event->addr.format() << HT_END;
}
