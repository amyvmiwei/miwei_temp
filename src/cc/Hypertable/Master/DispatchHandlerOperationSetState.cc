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

/** @file
 * Definitions for DispatchHandlerOperationSetState.
 * This file contains definitions for DispatchHandlerOperationSetState, a
 * dispatch handler class for gathering responses to a set of
 * RangeServer::set_state() requests.
 */

#include "Common/Compat.h"
#include "Common/Error.h"
#include "Common/Logger.h"
#include "Common/Time.h"

#include "Hypertable/Lib/StatsRangeServer.h"

#include "DispatchHandlerOperationSetState.h"

using namespace Hypertable;

DispatchHandlerOperationSetState::DispatchHandlerOperationSetState(ContextPtr &context) :
  DispatchHandlerOperation(context), m_timer(context->props->get_i32("Hypertable.Monitoring.Interval")) {
}

void DispatchHandlerOperationSetState::initialize() {
  m_context->system_state->get(m_specs, &m_generation);
  m_timer.start();
}


void DispatchHandlerOperationSetState::start(const String &location) {
  CommAddress addr;
  addr.set_proxy(location);
  m_rsclient.set_state(addr, m_specs, m_generation, this, m_timer);
}
