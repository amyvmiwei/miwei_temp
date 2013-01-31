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

#include "OperationRecoveryBlocker.h"
#include "GcWorker.h"

using namespace Hypertable;

OperationRecoveryBlocker::OperationRecoveryBlocker(ContextPtr &context)
    : Operation(context, MetaLog::EntityType::OPERATION_RECOVERY_BLOCKER) {
  m_obstructions.insert(Dependency::RECOVERY_BLOCKER);
}

OperationRecoveryBlocker::OperationRecoveryBlocker(ContextPtr &context,
        const MetaLog::EntityHeader &header_)
    : Operation(context, header_) {
  HT_ASSERT(!"Invalid OperationRecoveryBlocker constructor called");
}

void OperationRecoveryBlocker::execute() {

  HT_INFOF("Entering RecoveryBlocker-%lld", (Lld)header.id);

  size_t total_servers = m_context->rsc_manager->server_count();
  size_t connected_servers = m_context->available_server_count();
  size_t quorum = (total_servers * m_context->props->get_i32("Hypertable.Failover.Quorum.Percentage")) / 100;

  HT_INFO_OUT << "total_servers=" << total_servers << " connected_servers="
              << connected_servers << " quorum=" << quorum << HT_END;

  if (connected_servers < quorum || connected_servers == 0) {
    block();
    HT_INFO_OUT << "Only " << connected_servers
        << " servers ready, total servers=" << total_servers << " quorum="
        << quorum << ", wait for servers" << HT_END;
    HT_INFOF("Leaving RecoveryBlocker-%lld state=%s-BLOCKED", (Lld)header.id,
             OperationState::get_text(get_state()));
    return;
  }

  set_state(OperationState::COMPLETE);

  HT_INFOF("Leaving RecoveryBlocker-%lld state=%s", (Lld)header.id,
           OperationState::get_text(get_state()));
}

const String OperationRecoveryBlocker::name() {
  return "OperationRecoveryBlocker";
}

const String OperationRecoveryBlocker::label() {
  return "RecoveryBlocker";
}

