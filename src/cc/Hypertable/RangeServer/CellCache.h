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

#ifndef HYPERTABLE_CELLCACHE_H
#define HYPERTABLE_CELLCACHE_H

#include <Hypertable/RangeServer/CellCacheAllocator.h>
#include <Hypertable/RangeServer/CellListScanner.h>
#include <Hypertable/RangeServer/CellList.h>

#include <Hypertable/Lib/SerializedKey.h>

#include <Common/Mutex.h>

#include <map>
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
  class CellCache : public CellList {

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
     * (intrusive_ptr) to this CellCache.
     */
    virtual CellListScanner *create_scanner(ScanContextPtr &scan_ctx);

    void lock()   { m_mutex.lock(); }
    void unlock() { m_mutex.unlock(); }

    size_t size() { ScopedLock lock(m_mutex); return m_cell_map.size(); }

    bool empty() { ScopedLock lock(m_mutex); return m_cell_map.empty(); }

    /** Returns the amount of memory used by the CellCache.  This is the
     * summation of the lengths of all the keys and values in the map.
     */
    int64_t memory_used() {
      ScopedLock lock(m_mutex);
      return m_arena.used();
    }

    /**
     * Returns the amount of memory allocated by the CellCache.
     */
    uint64_t memory_allocated() {
      ScopedLock lock(m_mutex);
      return m_arena.total();
    }

    int64_t logical_size() {
      ScopedLock lock(m_mutex);
      return m_key_bytes + m_value_bytes;
    }

    void add_statistics(Statistics &stats) {
      ScopedLock lock(m_mutex);
      stats.size += m_cell_map.size();
      stats.deletes += m_deletes;
      stats.memory_used += m_arena.used();
      stats.memory_allocated += m_arena.total();
      stats.key_bytes += m_key_bytes;
      stats.value_bytes += m_value_bytes;
    }

    int32_t delete_count() {
      ScopedLock lock(m_mutex);
      return m_deletes;
    }

    void populate_key_set(KeySet &keys) {
      Key key;
      for (CellMap::const_iterator iter = m_cell_map.begin();
           iter != m_cell_map.end(); ++iter) {
       (*iter).first.load(key);
       keys.insert(key);
      }
    }

    CellCacheArena &arena() { return m_arena; }

  protected:

    friend class CellCacheScanner;

    class CellCacheKey : protected SerializedKey {
    public:
      explicit CellCacheKey(const uint8_t *buf)
        : SerializedKey(buf) { init(); }
      explicit CellCacheKey(const SerializedKey& key)
        : SerializedKey(key.ptr) { init(); }
      CellCacheKey(const CellCacheKey& cck)
        : SerializedKey(cck.ptr),
          extra(cck.extra), preffix(cck.preffix) { }
      inline bool operator<(const CellCacheKey& cck) const {
        return compare(cck) < 0;
      }
      inline const SerializedKey& key() const {
        return *this;
      }
      inline const char* row() const {
        return (const char*)ptr + offset() + 1;
      }
      inline uint32_t key_length() const {
        return length() + offset();
      }
      inline bool load(Key& key) const {
        return key.load(*this, length(), offset());
      }
      inline int compare(const CellCacheKey& cck) const {
        if (ptr == cck.ptr)
          return 0;
        if (preffix && cck.preffix && preffix != cck.preffix)
          return preffix < cck.preffix ? -1 : 1;
        const uint8_t* ptr1 = ptr + offset();
        const uint8_t* ptr2 = cck.ptr + cck.offset();
        int len1 = length();
        int len2 = cck.length();
        if (*ptr1 != *ptr2) {
          // see Key.h
          if (*ptr1 >= 0x80 && *ptr1 != 0xD0)
            len1 -= 8;
          if (*ptr2 >= 0x80 && *ptr2 != 0xD0)
            len2 -= 8;
        }
        int len = len1 < len2 ? len1 : len2;
        int cmp = memcmp(ptr1+1, ptr2+1, len-1);
        return cmp == 0 ? len1 - len2 : cmp;
      }
      inline int compare_counter(const CellCacheKey& cck) const {
        if (ptr == cck.ptr)
          return 0;
        if (preffix && cck.preffix && preffix != cck.preffix)
          return preffix < cck.preffix ? -1 : 1;
        const uint8_t* ptr1 = ptr + offset();
        const uint8_t* ptr2 = cck.ptr + cck.offset();
        int len1 = length();
        int len2 = cck.length();

        // Strip timestamp & revision from key length (see Key.h)
        if (*ptr1 >= 0x80)
          len1 -= (*ptr1 & Key::REV_IS_TS) ? 8 : 16;
        if (*ptr2 >= 0x80)
          len2 -= (*ptr2 & Key::REV_IS_TS) ? 8 : 16;

        int len = len1 < len2 ? len1 : len2;
        int cmp = memcmp(ptr1+1, ptr2+1, len-1);
        return cmp == 0 ? len1 - len2 : cmp;
      }
    private:
      enum { Mask = 0x3fffffff, Shift = 30 };
      inline int length() const { return extra & Mask; }
      inline int offset() const { return (extra >> Shift) + 1; }
      inline void init() {
        const uint8_t* next;
        int len = decode_length(&next) & Mask;
        extra = len | ((next - ptr - 1) << Shift);
        if (len > 1) {
          if (*++next) {
            preffix |= *next << 24;
            if (*++next) {
              preffix |= *next << 16;
              if (*++next) {
                preffix |= *next << 8;
                if (*++next)
                  preffix |= *next;
              }
            }
          }
        }
      }
      uint32_t extra;
      uint32_t preffix {};
    };
    typedef std::pair<const CellCacheKey, uint32_t> Value;
    typedef CellCacheAllocator<Value> Alloc;
    typedef std::map<const CellCacheKey, uint32_t,
                     std::less<const CellCacheKey>, Alloc> CellMap;

    struct CounterKeyCompare {
      bool operator()(const CellCache::Value& lhs, const CellCache::Value& rhs) {
        return lhs.first.compare_counter(rhs.first) < 0;
      }
    };

    Mutex m_mutex;
    CellCacheArena m_arena;
    CellMap m_cell_map;
    int32_t m_deletes {};
    int32_t m_collisions {};
    int64_t m_key_bytes {};
    int64_t m_value_bytes {};
    bool m_have_counter_deletes {};

  };

  typedef intrusive_ptr<CellCache> CellCachePtr;

} // namespace Hypertable;

#endif // HYPERTABLE_CELLCACHE_H
