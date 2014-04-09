/* -*- c++ -*-
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

/// @file
/// Declarations for ReferenceManager.
/// This file contains declarations for ReferenceManager, a class for tracking
/// manually removed operation.

#ifndef HYPERTABLE_REFERENCEMANAGER_H
#define HYPERTABLE_REFERENCEMANAGER_H

#include <Hypertable/Master/Operation.h>

#include <unordered_map>

namespace Hypertable {

  /// @addtogroup Master
  ///  @{

  /// Holds references to operations that are manually removed.
  /// This class was originally introduced to handle <i>move range</i>
  /// operations, but has since been used for any operation that needs to be
  /// manually removed.
  ///
  /// To motivate the original use case, a move range operation is initiated by
  /// a range server when it wants to give up a range.  It happens in two
  /// phases:
  ///
  ///   1. The range server calls MasterClient::move_range() which creates an
  ///      OperationMoveRange to handle the moving of a range to another range
  ///      server.
  ///   2. Once the range entity has been removed from the RSML, the range
  ///      server will call MasterClient::relinquish_acknowledge() to tell
  ///      the master that it has completely relinquished the range.
  ///
  /// This class was introduced to avoid a race condition in which a range server
  /// could create multiple OperationMoveRange operations for a range, causing it
  /// to be assigned to multiple servers.  When an OperationMoveRange gets
  /// created, it is added to the reference manager and won't be removed until
  /// the range server successfully completes the
  /// MasterClient::relinquish_acknowledge() call indicating that it has
  /// completely relinqished the range and will not call
  /// MasterClient::move_range() again for this range (unless it is later
  /// re-assigned to the server).  When a MOVE_RANGE command is received by
  /// the master, it will first check to see if the same move range operation
  /// exists in the ReferenceManager, and if so, it will not create another one,
  /// thus avoiding the race condition.
  class ReferenceManager {
  public:

    /// Adds an operation.
    /// @return Pointer to operation to add
    /// @return <i>true</i> if successfully added, <i>false</i> if not added
    /// because operation already exists in #m_map.
    bool add(Operation *operation);

    /// Adds an operation.
    /// @return Smart pointer to operation to add
    /// @return <i>true</i> if successfully added, <i>false</i> if not added
    /// because operation already exists in #m_map.
    bool add(OperationPtr &operation) { return add(operation.get()); }

    /// Looks up <code>hash_code</code> and returns associated operation.
    /// @return %Operation associated with <code>hash_code</code>, 0 otherwise
    OperationPtr get(int64_t hash_code);

    /// Remove operation associated with <code>hash_code</code>.
    /// @param hash_code Hash code of operation to remove.
    void remove(int64_t hash_code);

    /// Remove operation.
    /// @param operation Pointer to operation to remove
    void remove(Operation *operation) { remove(operation->hash_code()); }

    /// Remove operation.
    /// @param operation Smart pointer to operation to remove
    void remove(OperationPtr &operation) { remove(operation->hash_code()); }

    /// Clears map of all operations
    void clear();

  private:

    /// %Mutex for serializing concurrent access
    Mutex m_mutex;

    /// Reference map
    std::unordered_map<int64_t, OperationPtr> m_map;
  };

  /// @}

} // namespace Hypertable

#endif // HYPERTABLE_REFERENCEMANAGER_H
