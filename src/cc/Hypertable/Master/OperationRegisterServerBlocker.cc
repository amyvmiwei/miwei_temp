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

/** @file
 * Definitions for OperationRegisterServerBlocker.
 * This file contains definitions for OperationRegisterServerBlocker, an
 * Operation class for blocking server registration until a lock
 * release notificaiton for the server's file has been delivered from
 * Hyperspace.
 */

#include "Common/Compat.h"
#include "Common/Error.h"

#include "OperationProcessor.h"
#include "OperationRegisterServerBlocker.h"
#include "OperationTimedBarrier.h"

using namespace Hypertable;

OperationRegisterServerBlocker::OperationRegisterServerBlocker(ContextPtr &context,
                                                               const String &location)
  : OperationEphemeral(context, MetaLog::EntityType::OPERATION_REGISTER_SERVER_BLOCKER),
    m_location(location) {
  m_obstructions.insert(String("RegisterServerBlocker ") + m_location);
}

void OperationRegisterServerBlocker::execute() {
  int32_t state = get_state();

  HT_INFOF("Entering RegisterServerBlocker-%lld %s state=%s", (Lld)header.id,
           m_location.c_str(), OperationState::get_text(state));

  switch (state) {

  case OperationState::INITIAL:
    set_state(OperationState::STARTED);
    block();
    break;

  case OperationState::STARTED:
    complete_ok();
    break;

  default:
    HT_FATALF("Unrecognized state %d", state);
  }

  HT_INFOF("Leaving RegisterServerBlocker-%lld %s state=%s", (Lld)header.id,
           m_location.c_str(), OperationState::get_text(get_state()));
}

const String OperationRegisterServerBlocker::name() {
  return "OperationRegisterServerBlocker";
}

const String OperationRegisterServerBlocker::label() {
  return "RegisterServerBlocker(" + m_location + ")";
}

