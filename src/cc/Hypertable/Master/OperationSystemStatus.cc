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

#include "OperationSystemStatus.h"

#include <Hypertable/Master/DispatchHandlerOperationSystemStatus.h>
#include <Hypertable/Master/Utility.h>

#include <Common/Error.h>
#include <Common/Serialization.h>
#include <Common/Status.h>
#include <Common/StatusPersister.h>
#include <Common/StringExt.h>

#include <set>
#include <vector>

using namespace Hypertable;
using namespace Hypertable::Lib::Master;
using namespace std;

OperationSystemStatus::OperationSystemStatus(ContextPtr &context, EventPtr &event) 
  : OperationEphemeral(context, event, MetaLog::EntityType::OPERATION_STATUS) {
}

void OperationSystemStatus::execute() {

  HT_INFOF("Entering SystemStatus-%lld state=%s",
           (Lld)header.id, OperationState::get_text(get_state()));

  /// Shrink deadline by 25% to allow response to propagate back for timed out
  /// operations
  uint32_t deadline = (uint32_t)((double)m_event->header.timeout_ms * 0.75);
  Timer timer(deadline, true);
  Status status;

  while (Utility::status(m_context, timer, status)) {
    set<string> locations;
    vector<RangeServerConnectionPtr> servers;
    vector<DispatchHandlerOperationSystemStatus::Result> results;
    DispatchHandlerOperationSystemStatus dispatch_handler(m_context, timer);

    m_context->rsc_manager->get_servers(servers);
    if (servers.empty()) {
      status.set(Status::Code::CRITICAL, "No connected RangeServers");
      break;
    }
    results.reserve(servers.size());
    for (auto &rsc : servers) {
      DispatchHandlerOperationSystemStatus::Result result(rsc->location(), rsc->local_addr());
      results.push_back(result);
      locations.insert(result.location);
    }
    dispatch_handler.initialize(results);
    dispatch_handler.DispatchHandlerOperation::start(locations);
    dispatch_handler.wait_for_completion();

    for (auto &result : results) {
      Status::Code code;
      if (result.error != Error::OK) {
        if (result.error == Error::REQUEST_TIMEOUT) {
          if (status.get() != Status::Code::OK)
            continue;
          code = Status::Code::WARNING;
        }
        else
          code = Status::Code::CRITICAL;
        status.set(code, status_text_from_result(result));
        if (code == Status::Code::CRITICAL)
          break;
      }
      else if (result.status.get() != Status::Code::OK) {
        if (status.get() == Status::Code::OK ||
            (status.get() == Status::Code::WARNING &&
             result.status.get() == Status::Code::CRITICAL)) {
          status.set(result.status.get(),
                     status_text_from_result(result));                     
          if (status.get() == Status::Code::CRITICAL)
            break;
        }
      }
    }
    break;
  }

  if (status.get() == Status::Code::OK && !m_context->quorum_reached)
    status.set(Status::Code::WARNING,
               "RangeServer quorum has not been reached");

  m_params.set_status(status);
  complete_ok();

  HT_INFOF("Leaving SystemStatus-%lld %s", (Lld)header.id,
           status.format_output_line("Hypertable").c_str());
}

const String OperationSystemStatus::name() {
  return "OperationSystemStatus";
}

const String OperationSystemStatus::label() {
  return String("Status");
}

size_t OperationSystemStatus::encoded_result_length() const {
  return 4 + m_params.encoded_length();
}

/// @details
/// Encoding is as follows:
/// <table>
///   <tr>
///   <th>Encoding</th><th>Description</th>
///   </tr>
///   <tr>
///   <td>i32</td><td>Error code (Error::OK)</td>
///   </tr>
///   <tr>
///   <td>Response::Parameters::Status</td><td>Response parameters</td>
///   </tr>
/// </table>
void OperationSystemStatus::encode_result(uint8_t **bufp) const {
  Serialization::encode_i32(bufp, Error::OK);
  m_params.encode(bufp);
}


string OperationSystemStatus::status_text_from_result(DispatchHandlerOperationSystemStatus::Result &result) {
  Status::Code code;
  string address = result.addr.format_ipaddress();
  if (result.error != Error::OK) {
    if (result.error == Error::REQUEST_TIMEOUT)
      code = Status::Code::WARNING;
    else
      code = Status::Code::CRITICAL;
    if (!result.message.empty())
      return format("RangeServer %s (%s) %s - %s",
                    result.location.c_str(), address.c_str(),
                    Error::get_text(result.error), result.message.c_str());
    return format("RangeServer %s (%s) %s", result.location.c_str(),
                  address.c_str(), Error::get_text(result.error));
  }
  string text;
  result.status.get(&code, text);
  return format("RangeServer %s (%s) %s", result.location.c_str(),
                address.c_str(), text.c_str());
}
