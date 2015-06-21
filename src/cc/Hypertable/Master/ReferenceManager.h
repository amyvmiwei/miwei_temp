/* -*- c++ -*-
 * Copyright (C) 2007-2015 Hypertable, Inc.
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
/// Declarations for ReferenceManager.
/// This file contains declarations for ReferenceManager, a class for tracking
/// manually removed operation.

#ifndef Hypertable_Master_ReferenceManager_h
#define Hypertable_Master_ReferenceManager_h

#include <Hypertable/Master/Operation.h>

#include <mutex>
#include <unordered_map>

namespace Hypertable {

  /// @addtogroup Master
  /// @{

  /// Holds references to operations that are to be manually removed.
  class ReferenceManager {
  public:

    /// Adds an operation.
    /// Adds <code>operation</code> to #m_map using it's ID as the key.
    /// @param operation Smart pointer to operation to add
    /// @return <i>true</i> if successfully added, <i>false</i> if not added
    /// because operation already exists in #m_map.
    bool add(OperationPtr operation) {
      std::lock_guard<std::mutex> lock(m_mutex);
      if (m_map.find(operation->id()) != m_map.end())
        return false;
      m_map[operation->id()] = operation;
      return true;
    }

    /// Fetches an operation given its <code>id</code>.
    /// Looks up <code>id</code> in #m_map and returns the operation to which it
    /// maps.
    /// @return %Operation associated with <code>id</code>, nullptr if not found
    OperationPtr get(int64_t id) {
      std::lock_guard<std::mutex> lock(m_mutex);
      auto iter = m_map.find(id);
      if (iter == m_map.end())
        return 0;
      return (*iter).second;
    }

    /// Remove an operation.
    /// @param operation Operation for which to remove.
    void remove(OperationPtr operation) {
      std::lock_guard<std::mutex> lock(m_mutex);
      auto iter = m_map.find(operation->id());
      if (iter != m_map.end())
        m_map.erase(iter);
    }

    /// Clears all referenced operations.
    /// Clears #m_map.
    void clear() {
      std::lock_guard<std::mutex> lock(m_mutex);
      m_map.clear();
    }

  private:

    /// %Mutex for serializing concurrent access
    std::mutex m_mutex;

    /// Reference map
    std::unordered_map<int64_t, OperationPtr> m_map;
  };

  /// @}

} // namespace Hypertable

#endif // Hypertable_Master_ReferenceManager_h
