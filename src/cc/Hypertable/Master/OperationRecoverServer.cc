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
#include "Common/md5.h"

#include "OperationRecoverServer.h"

using namespace Hypertable;

OperationRecoverServer::OperationRecoverServer(ContextPtr &context, RangeServerConnectionPtr &rsc)
  : Operation(context, MetaLog::EntityType::OPERATION_RECOVER_SERVER), m_rsc(rsc) {
  m_exclusivities.insert(m_rsc->location());
  m_hash_code = md5_hash("RecoverServer") ^ md5_hash(m_rsc->location().c_str());
}


void OperationRecoverServer::execute() {

  if (!m_rsc->connected()) {
    block();
    return;
  }

  set_state(OperationState::COMPLETE);

  HT_INFOF("Leaving RecoverServer-%lld('%s') state=%s",
           (Lld)header.id, m_rsc->location().c_str(), OperationState::get_text(get_state()));
}


void OperationRecoverServer::display_state(std::ostream &os) {
  os << " location=" << m_rsc->location() << " ";
}

const String OperationRecoverServer::name() {
  return "OperationRecoverServer";
}

const String OperationRecoverServer::label() {
  return String("RecoverServer ") + m_rsc->location();
}

