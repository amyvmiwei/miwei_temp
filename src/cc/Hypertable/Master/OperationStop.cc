/*
 * Copyright (C) 2007-2013 Hypertable, Inc.
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
#include "Common/FailureInducer.h"
#include "Common/ScopeGuard.h"
#include "Common/Serialization.h"
#include "Common/StringExt.h"
#include "Common/System.h"
#include "Common/md5.h"

#include "OperationProcessor.h"
#include "OperationStop.h"

using namespace Hypertable;

OperationStop::OperationStop(ContextPtr &context, EventPtr &event) 
  : OperationEphemeral(context, event, MetaLog::EntityType::OPERATION_STOP) {
  const uint8_t *ptr = event->payload;
  size_t remaining = event->payload_len;
  decode_request(&ptr, &remaining);
  m_dependencies.insert(Dependency::INIT);
}

void OperationStop::execute() {
  HT_INFOF("Entering OperationStop-%s recover=%s state=%s",
          m_server.c_str(), m_recover ? "true" : "false",
          OperationState::get_text(m_state));

  try {
    RangeServerClient rsc(m_context->comm);
    CommAddress addr;

    addr.set_proxy(m_server);
    rsc.shutdown(addr);
  }
  catch (Exception &e) {
    if (e.code() == Error::COMM_INVALID_PROXY
            || e.code() == Error::COMM_NOT_CONNECTED)
      complete_error(Error::RANGESERVER_NOT_FOUND, m_server);
    else
      complete_error(e.code(), m_server);
    return;
  }

  complete_ok();

  HT_INFOF("Leaving OperationStop-%s recover=%s state=%s",
          m_server.c_str(), m_recover ? "true" : "false",
          OperationState::get_text(m_state));
}

void OperationStop::display_state(std::ostream &os) {
  os << " " << m_server << " recover=" << m_recover;
}

void OperationStop::decode_request(const uint8_t **bufp, size_t *remainp) {
  m_server = Serialization::decode_vstr(bufp, remainp);
  m_recover = Serialization::decode_bool(bufp, remainp);
}

const String OperationStop::name() {
  return "OperationStop";
}

const String OperationStop::label() {
  return format("OperationStop %s", m_server.c_str());
}
