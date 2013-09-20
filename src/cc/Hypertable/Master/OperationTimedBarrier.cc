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
#include "OperationTimedBarrier.h"

using namespace Hypertable;

OperationTimedBarrier::OperationTimedBarrier(ContextPtr &context,
                                             const String &block_dependency,
                                             const String &wakeup_dependency)
  : OperationEphemeral(context, MetaLog::EntityType::OPERATION_TIMED_BARRIER),
    m_block_dependency(block_dependency), m_wakeup_dependency(wakeup_dependency),
    m_shutdown(false) {
  m_obstructions.insert(block_dependency);
  boost::xtime_get(&m_expire_time, boost::TIME_UTC_);
}

void OperationTimedBarrier::execute() {
  ScopedLock lock(m_mutex);

  HT_INFOF("Entering TimedBarrier-%lld state=%s", (Lld)header.id,
           OperationState::get_text(m_state));

  HiResTime now;
  while (now < m_expire_time && !m_shutdown) {
    HT_INFOF("Barrier for %s will be up for %lld milliseconds",
             m_block_dependency.c_str(), (Lld)xtime_diff_millis(now, m_expire_time));
    m_cond.timed_wait(lock, (boost::xtime)m_expire_time);
    now.reset();
  }

  m_context->op->activate(m_wakeup_dependency);

  m_state = OperationState::COMPLETE;

  HT_INFOF("Leaving TimedBarrier-%lld state=%s", (Lld)header.id,
           OperationState::get_text(m_state));
}

const String OperationTimedBarrier::name() {
  return "OperationTimedBarrier";
}

const String OperationTimedBarrier::label() {
  return "TimedBarrier";
}

void OperationTimedBarrier::advance_into_future(uint32_t millis) {
  ScopedLock lock(m_mutex);
  HiResTime new_time;

  new_time += millis;

  if (m_expire_time < new_time)
    m_expire_time = new_time;
}

void OperationTimedBarrier::shutdown() {
  ScopedLock lock(m_mutex);
  m_shutdown = true;
  m_cond.notify_all();
}
