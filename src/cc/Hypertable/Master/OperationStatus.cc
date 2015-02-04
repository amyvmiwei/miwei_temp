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

#include "OperationStatus.h"

#include <Common/Error.h>
#include <Common/Serialization.h>
#include <Common/StringExt.h>

using namespace Hypertable;
using namespace Hypertable::Lib::Master;

OperationStatus::OperationStatus(ContextPtr &context, EventPtr &event) 
  : OperationEphemeral(context, event, MetaLog::EntityType::OPERATION_STATUS) {
  HT_INFOF("Status-%lld", (Lld)header.id);
}

void OperationStatus::execute() {
  Status status;

  if (m_context->startup_in_progress())
    status.set(Status::Code::WARNING, "server is coming up");
  else if (m_context->shutdown_in_progress())
    status.set(Status::Code::WARNING, "server is shutting down");
  else if (!m_context->master_file->lock_acquired())
    status.set(Status::Code::OK, "standby");
  else if (m_context->quorum_reached) {
    size_t connected_servers = m_context->available_server_count();
    size_t total_servers = m_context->rsc_manager->server_count();
    if (connected_servers < total_servers) {
      size_t failover_pct
        = m_context->props->get_i32("Hypertable.Failover.Quorum.Percentage");
      size_t quorum = ((total_servers * failover_pct) + 99) / 100;
      if (connected_servers == 0)
        status.set(Status::Code::CRITICAL,
                   "RangeServer recovery blocked because 0 servers available.");
      else if (connected_servers < quorum)
        status.set(Status::Code::CRITICAL,
                   format("RangeServer recovery blocked (%d servers "
                          "available, quorum of %d is required)",
                          (int)connected_servers, (int)quorum));
      else
        status.set(Status::Code::WARNING, "RangeServer recovery in progress");
    }
  }
  m_params.set_status(status);
  complete_ok();
}

const String OperationStatus::name() {
  return "OperationStatus";
}

const String OperationStatus::label() {
  return String("Status");
}

size_t OperationStatus::encoded_result_length() const {
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
void OperationStatus::encode_result(uint8_t **bufp) const {
  Serialization::encode_i32(bufp, Error::OK);
  m_params.encode(bufp);
}
