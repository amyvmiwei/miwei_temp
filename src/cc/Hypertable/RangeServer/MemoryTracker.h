/* -*- c++ -*-
 * Copyright (C) 2007-2014 Hypertable, Inc.
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

/// @file
/// Declarations for MemoryTracker.
/// This file contains type declarations for MemoryTracker, a class used to
/// track range server memory used.

#ifndef HYPERTABLE_MEMORYTRACKER_H
#define HYPERTABLE_MEMORYTRACKER_H

#include <Hypertable/RangeServer/FileBlockCache.h>
#include <Hypertable/RangeServer/QueryCache.h>

#include <boost/thread/mutex.hpp>

#include <memory>

namespace Hypertable {

  /// @addtogroup RangeServer
  /// @{

  /// Tracks range server memory used.
  class MemoryTracker {
  public:

    /// Constructor.
    /// @param block_cache Pointer to block cache
    /// @param query_cache Pointer to query cache
    MemoryTracker(FileBlockCache *block_cache, QueryCachePtr query_cache) 
      : m_block_cache(block_cache), m_query_cache(query_cache) { }

    /// Add to memory used.
    /// @param amount Amount of memory to add
    void add(int64_t amount) {
      ScopedLock lock(m_mutex);
      m_memory_used += amount;
    }

    /// Subtract to memory used.
    /// @param amount Amount of memory to subtract
    void subtract(int64_t amount) {
      ScopedLock lock(m_mutex);
      m_memory_used -= amount;
    }

    /// Return total range server memory used.
    /// This member function returns the total amount of memory used, computed
    /// as #m_memory_used plus block cache memory used plus query cache memory
    /// used.
    /// @return Total range server memory used
    int64_t balance() {
      ScopedLock lock(m_mutex);
      return m_memory_used + (m_block_cache ? m_block_cache->memory_used() : 0) +
        (m_query_cache ? m_query_cache->memory_used() : 0);
    }

  private:

    /// %Mutex to serialize concurrent access
    Mutex m_mutex;

    /// Current range server memory used
    int64_t m_memory_used {};

    /// Pointer to block cache
    FileBlockCache *m_block_cache;

    /// Pointer to query cache
    QueryCachePtr m_query_cache;
  };

  /// @}

}

#endif // HYPERTABLE_MEMORYTRACKER_H
