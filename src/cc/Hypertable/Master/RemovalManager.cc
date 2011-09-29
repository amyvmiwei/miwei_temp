/** -*- c++ -*-
 * Copyright (C) 2011 Hypertable, Inc.
 *
 * This file is part of Hypertable.
 *
 * Hypertable is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
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

#include "Common/Compat.h"

#include "RemovalManager.h"

using namespace Hypertable;


void RemovalManager::add_operation(Operation *operation, size_t needed_approvals) {
  ScopedLock lock(m_mutex);

  if (m_map.find(operation->hash_code()) != m_map.end())
    return;

  m_map[operation->hash_code()] = RemovalRec(operation, needed_approvals);
}

void RemovalManager::approve_removal(int64_t hash_code) {
  ScopedLock lock(m_mutex);
  RemovalMapT::iterator iter = m_map.find(hash_code);
  HT_ASSERT(iter != m_map.end());
  HT_ASSERT((*iter).second.approvals_remaining > 0);
  (*iter).second.approvals_remaining--;
  if ((*iter).second.approvals_remaining == 0) {
    m_metalog_writer->record_removal((*iter).second.op.get());
    m_map.erase(iter);
  }
}


bool RemovalManager::is_present(int64_t hash_code) {
  ScopedLock lock(m_mutex);
  return m_map.find(hash_code) != m_map.end();
}


void RemovalManager::clear() {
  ScopedLock lock(m_mutex);
  m_map.clear();
}
