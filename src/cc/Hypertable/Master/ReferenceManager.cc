/*
 * Copyright (C) 2007-2012 Hypertable, Inc.
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

/// @file
/// Definitions for ReferenceManager.
/// This file contains definitions for ReferenceManager, a class for tracking
/// manually removed operation.

#include <Common/Compat.h>
#include "ReferenceManager.h"

using namespace Hypertable;

bool ReferenceManager::add(K key, OperationPtr operation) {
  ScopedLock lock(m_mutex);

  if (m_map.find(key) != m_map.end())
    return false;
  
  m_map[key] = operation;
  return true;
}

OperationPtr ReferenceManager::get(K key) {
  ScopedLock lock(m_mutex);
  auto iter = m_map.find(key);
  if (iter == m_map.end())
    return 0;
  return (*iter).second;
}

bool ReferenceManager::exists(K key) {
  ScopedLock lock(m_mutex);
  auto iter = m_map.find(key);
  return iter != m_map.end();
}


void ReferenceManager::remove(K key) {
  ScopedLock lock(m_mutex);
  auto iter = m_map.find(key);
  if (iter != m_map.end())
    m_map.erase(iter);
}
