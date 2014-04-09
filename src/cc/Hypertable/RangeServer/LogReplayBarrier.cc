/*
 * Copyright (C) 2007-2014 Hypertable, Inc.
 *
 * This file is part of Hypertable.
 *
 * Hypertable is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 3
 * of the License, or any later version.
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
#include "LogReplayBarrier.h"

#include <Hypertable/Lib/Key.h>

using namespace Hypertable;

void LogReplayBarrier::set_root_complete() {
  ScopedLock lock(m_mutex);
  m_root_complete = true;
  m_root_complete_cond.notify_all();
}

void LogReplayBarrier::set_metadata_complete() {
  ScopedLock lock(m_mutex);
  m_metadata_complete = true;
  m_metadata_complete_cond.notify_all();
}

void LogReplayBarrier::set_system_complete() {
  ScopedLock lock(m_mutex);
  m_system_complete = true;
  m_system_complete_cond.notify_all();
}

void LogReplayBarrier::set_user_complete() {
  ScopedLock lock(m_mutex);
  m_user_complete = true;
  m_user_complete_cond.notify_all();
}

bool LogReplayBarrier::wait_for_root(boost::xtime deadline) {
  if (m_root_complete)
    return true;
  ScopedLock lock(m_mutex);
  while (!m_root_complete) {
    HT_INFO("Waiting for ROOT recovery to complete...");
    if (!m_root_complete_cond.timed_wait(lock, deadline))
      return false;
  }
  return true;
}

bool LogReplayBarrier::wait_for_metadata(boost::xtime deadline) {
  if (m_metadata_complete)
    return true;
  ScopedLock lock(m_mutex);
  while (!m_metadata_complete) {
    HT_INFO("Waiting for METADATA recovery to complete...");
    if (!m_metadata_complete_cond.timed_wait(lock, deadline))
      return false;
  }
  return true;
}

bool LogReplayBarrier::wait_for_system(boost::xtime deadline) {
  if (m_system_complete)
    return true;
  ScopedLock lock(m_mutex);
  while (!m_system_complete) {
    HT_INFO("Waiting for SYSTEM recovery to complete...");
    if (!m_system_complete_cond.timed_wait(lock, deadline))
      return false;
  }
  return true;
}

bool LogReplayBarrier::wait_for_user(boost::xtime deadline) {
  if (m_user_complete)
    return true;
  ScopedLock lock(m_mutex);
  while (!m_user_complete) {
    HT_INFO("Waiting for USER recovery to complete...");
    if (!m_user_complete_cond.timed_wait(lock, deadline))
      return false;
  }
  return true;
}

bool
LogReplayBarrier::wait(boost::xtime deadline,
                      const TableIdentifier *table,
                      const RangeSpec *range_spec) {
  if (m_user_complete)
    return true;
  if (table->is_metadata()) {
    if (range_spec && !strcmp(range_spec->end_row, Key::END_ROOT_ROW))
      return wait_for_root(deadline);
    else
      return wait_for_metadata(deadline);
  }
  else if (table->is_system())
    return wait_for_system(deadline);
  return wait_for_user(deadline);
}

bool LogReplayBarrier::user_complete() {
  ScopedLock lock(m_mutex);
  return m_user_complete;
}
