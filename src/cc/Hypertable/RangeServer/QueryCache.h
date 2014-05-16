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

#ifndef Hypertable_RangeServer_QueryCache_h
#define Hypertable_RangeServer_QueryCache_h

#include <Common/Checksum.h>
#include <Common/Mutex.h>
#include <Common/atomic.h>

#include <boost/multi_index_container.hpp>
#include <boost/multi_index/hashed_index.hpp>
#include <boost/multi_index/mem_fun.hpp>
#include <boost/multi_index/sequenced_index.hpp>
#include <boost/multi_index/member.hpp>
#include <boost/shared_array.hpp>

#include <algorithm>
#include <cstring>
#include <set>

namespace Hypertable {
  using namespace boost::multi_index;

  /// @addtogroup RangeServer
  /// @{

  /// Query cache.
  class QueryCache {

  public:

    /// Hash key to query cache.
    /// This class represents a hash key, computed over a query specification,
    /// used to quickly lookup the cache entry for the query.
    class Key {
    public:
      /// Equality operator.
      /// @param other Other key to compare
      /// @return <i>true</i> if keys are equal, <i>false</i> otherwise.
      bool operator==(const Key &other) const {
	return memcmp(digest, other.digest, 16) == 0;
      }
      /// Hash digest.
      uint64_t digest[2];
    };

    /// Row key information.
    class RowKey {
    public:
      /// Constructor.
      /// Initializes #tablename to <code>t</code> and #row to <code>r</code>.
      /// This member function assumes that <code>t</code> and <code>r</code>
      /// are pointers to memory that will be valid for the lifetime of the
      /// cache entry.  Also sets #hash to the fletcher32 checksum of the
      /// tablename and row for invalidation purposes.
      /// @param t Table name
      /// @param r Row
      RowKey(const char *t, const char *r) : tablename(t), row(r) {
	hash = fletcher32(t, strlen(t)) ^ fletcher32(r, strlen(r));
      }
      /// Equality operator.
      /// @param other Other key to compare
      /// @return <i>true</i> if keys are equal, <i>false</i> otherwise.
      bool operator==(const RowKey &other) const {
	return !strcmp(tablename, other.tablename) && !strcmp(row, other.row);
      }
      /// %Table name
      const char *tablename;
      /// Row
      const char *row;
      /// Hash code computed from #tablename and #row
      uint32_t hash;
    };

    /// Constructor.
    /// Initializes #m_max_memory and #m_avail_memory to
    /// <code>max_memory</code>.
    /// @param max_memory Maximum amount of memory to be used by the cache
    QueryCache(uint64_t max_memory)
      : m_max_memory(max_memory), m_avail_memory(max_memory) { }

    /// Inserts a query result.
    /// If the size of the entry is greater than #m_max_memory, then the
    /// function returns without modifying the cache.  Then the old entry is
    /// removed, if there was one.  Then room is created in the cache for the
    /// new entry by removing the oldest entries until enough space is
    /// available.  Finally, a new cache entry is created and inserted into the
    /// cache.  This function also maintains the #m_available_memory value which
    /// represents how much room is available in the cache.  It is computed as
    /// #m_max_memory minus an approximation of how much space is taken up by
    /// the existing cache entries.
    /// @param key Hash key for entry to be inserted
    /// @param tablename %Table name for entry to be inserted (must remain valid
    /// for lifetime of cache entry)
    /// @param row Row of entry to be inserted (must remain valid for lifetime
    /// of cache entry)
    /// @param columns Set of column IDs from scan specification used to create
    /// entry to be inserted
    /// @param cell_count Count of cells in entry to be inserted
    /// @param result Query result
    /// @param result_length Length of query result
    /// @return <i>true</i> if result was inserted, <i>false</i> otherwise.
    bool insert(Key *key, const char *tablename, const char *row,
                std::set<uint8_t> &columns, uint32_t cell_count,
                boost::shared_array<uint8_t> &result, uint32_t result_length);

    /// Lookup.
    /// Looks up the entry with key <code>key</code>, and if found, returns the
    /// query result and associated information in <code>result</code>,
    /// <code>lenp</code>, and <code>cell_count</code>.  Also, if a cache entry
    /// is found, it is removed and re-inserted for LRU ordering.
    /// @param key Hash key
    /// @param result Reference to shared array to hold result
    /// @param lenp Pointer to variable to hold result length
    /// @param cell_count Pointer to variable to hold count of cells in result
    /// @return <i>true</i> if an entry was found, <i>false</i> otherwise
    bool lookup(Key *key, boost::shared_array<uint8_t> &result, uint32_t *lenp,
                uint32_t *cell_count);

    /// Invalidates cache entries.
    /// Creates a RowKey object from <code>tablename</code> and <code>row</code>
    /// and finds all matching entires in the cache.  For each matching cache
    /// entry whose columns intersect with <code>columns</code>, the entry is
    /// invalidated.  The entry is also invalidated if either
    /// <code>columns</code> is empty or the cache entries columns are empty.
    /// @param tablename %Table of entries to invalidate
    /// @param row Row entries to invalidate
    /// @param columns Columns of entries to invalidate
    void invalidate(const char * tablename, const char *row, std::set<uint8_t> &columns);

    /// Gets available memory.
    /// Returns #m_avail_memory
    /// @return Available memory
    uint64_t available_memory() { ScopedLock lock(m_mutex); return m_avail_memory; }

    /// Gets memory used.
    /// Memory used is calculated as #m_max_memory minus #m_avail_memory.
    /// @return Memory used
    uint64_t memory_used() { ScopedLock lock(m_mutex); return m_max_memory-m_avail_memory; }

    /// Gets cache statistics.
    /// @param max_memoryp Address of variable to hold <i>max memory</i>.
    /// @param available_memoryp Address of variable to hold <i>available memory</i>.
    /// @param total_lookupsp Address of variable to hold <i>total lookups</i>.
    /// @param total_hitsp Address of variable to hold <i>total hits</i>.
    void get_stats(uint64_t *max_memoryp, uint64_t *available_memoryp,
                   uint64_t *total_lookupsp, uint64_t *total_hitsp);

  private:

    /// Internal cache entry.
    class QueryCacheEntry {
    public:
      QueryCacheEntry(Key &k, const char *tname, const char *rw,
                      std::set<uint8_t> &column_ids, uint32_t cells,
		      boost::shared_array<uint8_t> &res, uint32_t rlen) :
	key(k), row_key(tname, rw), result(res), result_length(rlen), cell_count(cells) {
        columns.swap(column_ids);
      }
      Key lookup_key() const { return key; }
      RowKey invalidate_key() const { return row_key; }
      void dump() { std::cout << row_key.tablename << ":" << row_key.row << "\n"; }
      Key key;
      RowKey row_key;
      std::set<uint8_t> columns;
      boost::shared_array<uint8_t> result;
      uint32_t result_length;
      uint32_t cell_count;
    };

    struct KeyHash {
      std::size_t operator()(const Key k) const {
	return (std::size_t)k.digest[0];
      }
    };

    struct RowKeyHash {
      std::size_t operator()(const RowKey k) const {
        return k.hash;
      }
    };

    typedef boost::multi_index_container<
      QueryCacheEntry,
      indexed_by<
        sequenced<>,
        hashed_unique<const_mem_fun<QueryCacheEntry, Key,
		      &QueryCacheEntry::lookup_key>, KeyHash>,
        hashed_non_unique<const_mem_fun<QueryCacheEntry, RowKey,
		          &QueryCacheEntry::invalidate_key>, RowKeyHash>
      >
    > Cache;

    typedef Cache::nth_index<0>::type Sequence;
    typedef Cache::nth_index<1>::type LookupHashIndex;
    typedef Cache::nth_index<2>::type InvalidateHashIndex;

    /// %Mutex to serialize member access
    Mutex m_mutex;

    /// Internal cache data structure
    Cache m_cache;

    /// Maximum memory to be used by cache
    uint64_t m_max_memory {};

    /// Available memory
    uint64_t m_avail_memory {};
    
    /// Total lookup count
    uint64_t m_total_lookup_count {};

    /// Total hit count
    uint64_t m_total_hit_count {};

    /// Recent hit count (for logging)
    uint32_t m_recent_hit_count {};
  };

  /// Smart pointer to QueryCache
  typedef std::shared_ptr<QueryCache> QueryCachePtr;

  /// @}

}


#endif // Hypertable_RangeServer_QueryCache_h
