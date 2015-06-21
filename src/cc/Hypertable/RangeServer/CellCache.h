/* -*- c++ -*-
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

#ifndef Hypertable_RangeServer_CellCache_h
#define Hypertable_RangeServer_CellCache_h

#include <Hypertable/RangeServer/CellCacheAllocator.h>
#include <Hypertable/RangeServer/CellListScanner.h>
#include <Hypertable/RangeServer/CellList.h>

#include <Hypertable/Lib/SerializedKey.h>

#include <map>
#include <memory>
#include <mutex>
#include <set>

namespace Hypertable {

  struct key_revision_lt {
    bool operator()(const Key &k1, const Key &k2) const {
      return k1.revision < k2.revision;
    }
  };

  typedef std::set<Key, key_revision_lt> KeySet;


  /**
   * Represents  a sorted list of key/value pairs in memory.
   * All updates get written to the CellCache and later get "compacted"
   * into a CellStore on disk.
   */
  class CellCache : public CellList, public std::enable_shared_from_this<CellCache> {

  public:

    /// Holds cache statistics.
    struct Statistics {
      size_t size {};
      size_t deletes {};
      int64_t memory_used {};
      int64_t memory_allocated {};
      int64_t key_bytes {};
      int64_t value_bytes {};
    };

    CellCache();
    CellCache(CellCacheArena &arena);
    virtual ~CellCache() { m_cell_map.clear(); }
    /**
     * Adds a key/value pair to the CellCache.  This method assumes that
     * the CellCache has been locked by a call to #lock.  Copies of
     * the key and value are created and inserted into the underlying cell map
     *
     * @param key key to be inserted
     * @param value value to inserted
     */
    virtual void add(const Key &key, const ByteString value);

    virtual void add_counter(const Key &key, const ByteString value);

    virtual void split_row_estimate_data(SplitRowDataMapT &split_row_data);

    /** Creates a CellCacheScanner object that contains an shared pointer
     * to this CellCache.
     */
    CellListScannerPtr create_scanner(ScanContext *scan_ctx) override;

    void lock()   { m_mutex.lock(); }
    void unlock() { m_mutex.unlock(); }

    size_t size() { std::lock_guard<std::mutex> lock(m_mutex); return m_cell_map.size(); }

    bool empty() { std::lock_guard<std::mutex> lock(m_mutex); return m_cell_map.empty(); }

    /** Returns the amount of memory used by the CellCache.  This is the
     * summation of the lengths of all the keys and values in the map.
     */
    int64_t memory_used() {
      std::lock_guard<std::mutex> lock(m_mutex);
      return m_arena.used();
    }

    /**
     * Returns the amount of memory allocated by the CellCache.
     */
    uint64_t memory_allocated() {
      std::lock_guard<std::mutex> lock(m_mutex);
      return m_arena.total();
    }

    int64_t logical_size() {
      std::lock_guard<std::mutex> lock(m_mutex);
      return m_key_bytes + m_value_bytes;
    }

    void add_statistics(Statistics &stats) {
      std::lock_guard<std::mutex> lock(m_mutex);
      stats.size += m_cell_map.size();
      stats.deletes += m_deletes;
      stats.memory_used += m_arena.used();
      stats.memory_allocated += m_arena.total();
      stats.key_bytes += m_key_bytes;
      stats.value_bytes += m_value_bytes;
    }

    int32_t delete_count() {
      std::lock_guard<std::mutex> lock(m_mutex);
      return m_deletes;
    }

    void populate_key_set(KeySet &keys) {
      Key key;
      for (CellMap::const_iterator iter = m_cell_map.begin();
	   iter != m_cell_map.end(); ++iter) {
	key.load((*iter).first);
	keys.insert(key);
      }
    }

    CellCacheArena &arena() { return m_arena; }

    friend class CellCacheScanner;

    typedef std::pair<const SerializedKey, uint32_t> Value;
    typedef CellCacheAllocator<Value> Alloc;
    typedef std::map<const SerializedKey, uint32_t,
                     std::less<const SerializedKey>, Alloc> CellMap;

  protected:

    std::mutex m_mutex;
    CellCacheArena m_arena;
    CellMap m_cell_map;
    int32_t m_deletes {};
    int32_t m_collisions {};
    int64_t m_key_bytes {};
    int64_t m_value_bytes {};
    bool m_have_counter_deletes {};

  };

  /// Shared smart pointer to CellCache
  typedef std::shared_ptr<CellCache> CellCachePtr;

}

#endif // Hypertable_RangeServer_CellCache_h
