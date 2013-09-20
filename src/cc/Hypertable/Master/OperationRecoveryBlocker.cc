/* -*- c++ -*-
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

#include "OperationProcessor.h"
#include "OperationRecoveryBlocker.h"
#include "OperationTimedBarrier.h"

using namespace Hypertable;

OperationRecoveryBlocker::OperationRecoveryBlocker(ContextPtr &context)
  : OperationEphemeral(context, MetaLog::EntityType::OPERATION_RECOVERY_BLOCKER) {
  m_obstructions.insert(Dependency::RECOVERY_BLOCKER);
}

void OperationRecoveryBlocker::execute() {

  HT_INFOF("Entering RecoveryBlocker-%lld", (Lld)header.id);

  size_t total_servers = m_context->rsc_manager->server_count();
  size_t connected_servers = m_context->available_server_count();
  size_t failover_pct
    = m_context->props->get_i32("Hypertable.Failover.Quorum.Percentage");
  size_t quorum = ((total_servers * failover_pct) + 99) / 100;

  HT_INFOF("total_servers=%d connected_servers=%d quorum=%d",
           (int)total_servers, (int)connected_servers, (int)quorum);

  if (connected_servers < quorum || connected_servers == 0) {
    block();
    HT_INFOF("Only %d servers ready, total servers=%d quorum=%d,"
             " waiting for servers ...", (int)connected_servers,
             (int)total_servers, (int)quorum);
    HT_INFOF("Leaving RecoveryBlocker-%lld state=%s-BLOCKED", (Lld)header.id,
             OperationState::get_text(get_state()));
    return;
  }

  /**
   * This statement prevents recovery from being triggered erroneously during
   * startup.  If Context::quorum_reached is false, that means the Master is
   * in the process of starting up.  Once the quorum has been reached, then
   * Context::quorum_reached gets set to true and the TimedBarrier operation
   * gets added to the OperationProcessor which gives the remaining servers
   * a chance to connect without being recovered.
   */
  if (!m_context->quorum_reached) {
    uint32_t millis = 2 * m_context->props->get_i32("Hypertable.Failover.GracePeriod");
    m_context->recovery_barrier_op->advance_into_future(millis);
    OperationPtr operation = m_context->recovery_barrier_op;
    m_context->op->add_operation(operation);
    m_context->quorum_reached = true;
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

