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
/// Declarations for CellCacheManager.
/// This file contains type declarations for CellCacheManager, a class for
/// managing an access group's cell caches.

#ifndef Hypertable_RangeServer_CellCacheManager_h
#define Hypertable_RangeServer_CellCacheManager_h

#include <Hypertable/RangeServer/CellCache.h>
#include <Hypertable/RangeServer/CellList.h>
#include <Hypertable/RangeServer/CellListScanner.h>
#include <Hypertable/RangeServer/MergeScannerAccessGroup.h>
#include <Hypertable/RangeServer/ScanContext.h>

#include <Hypertable/Lib/Schema.h>

#include <Common/ReferenceCount.h>

namespace Hypertable {

  /// @addtogroup RangeServer
  /// @{

  /// Manages the cell cache of an access group.
  /// When an access group undergoes a compaction, the cell cache needs to be
  /// frozen (i.e. made immutable) before it is written out to a cell store
  /// file.  This class manages this process by maintaining two caches, an
  /// active cache  that receives inserts, and an immutable cache, created when
  /// the active cache is frozen in preparation for a compaction.  It provides
  /// member functions for freezing the active cache to the immutable cache,
  /// dropping the immutable cache when it is no longer needed, and merging the
  /// two caches back together if a compaction was aborted.
  class CellCacheManager : public ReferenceCount {

  public:

    /// Constructor.
    /// Initializes #m_active_cache with a newly allocated CellCache
    CellCacheManager() : m_active_cache{new CellCache()} { }

    /// Destructor.
    virtual ~CellCacheManager() { }

    /// Installs a new active cache.
    /// This function replaces #m_active_cache with <code>new_cache</code>.
    /// @param new_cache New active cache
    void install_new_active_cache(CellCachePtr new_cache) {
      m_active_cache = new_cache;
    }

    /// Installs a new immutable cache.
    /// This function replaces #m_immutable_cache with <code>new_cache</code>.
    /// @param new_cache New immutable cache
    void install_new_immutable_cache(CellCachePtr new_cache) {
      m_immutable_cache = new_cache;
    }

    /// Merges immutable cache into active cache.
    void merge_caches(SchemaPtr &schema);

    /// Inserts a key/value pair to the active cache.
    /// Adds <code>key</code> and <code>value</code> directly into
    /// #m_active_cache.
    /// @note lock() must be called before calling this function.
    /// @param key key to be inserted
    /// @param value value to inserted
    void add(const Key &key, const ByteString value) {
      m_active_cache->add(key, value);
    }

    /// Inserts a key/value pair for a counter column into the active cache.
    /// Adds <code>key</code> and <code>value</code> directly into
    /// #m_active_cache.
    /// @note lock() must be called before calling this function.
    /// @param key key to be inserted
    /// @param value value to inserted
    void add_counter(const Key &key, const ByteString value) {
      m_active_cache->add_counter(key, value);
    }

    /// Inserts key/value pairs from a scanner into the active cache.
    /// This method retrieves all key/value pairs from <code>scanner</code> and
    /// inserts them into the active cache.
    /// @param scanner Scanner from which to fetch key/value pairs
    void add(CellListScannerPtr &scanner);

    /// Creates a scanner on the immutable cache and adds it to a merge scanner.
    /// If an immutable cache is installed, a scanner is created on it and added
    /// to <code>mscanner</code>.
    /// @param mscanner Merge scanner to which immutable cache scanner should be
    /// added
    /// @param scan_ctx Scan context for initializing immutable cache scanner
    void add_immutable_scanner(MergeScannerAccessGroup *mscanner,
                               ScanContextPtr &scan_ctx);

    /// Creates scanners on the active and immutable caches and adds them to a
    /// merge scanner.  If the active cache is not empty, a scanner is
    /// created on it and added to <code>mscanner</code>.  If an immutable cache
    /// is installed, a scanner is created on it and added to
    /// <code>mscanner</code> as well.
    /// @param mscanner Merge scanner to which immutable cache scanner should be
    /// added
    /// @param scan_ctx Scan context for initializing scanners
    void add_scanners(MergeScannerAccessGroup *scanner
                      , ScanContextPtr &scan_context);

    /// Populates map of split row data.
    /// This method calls CellCache::split_row_estimate_data() on both the
    /// active and immutable caches to add data to <code>split_row_data</code>.
    /// @param split_row_data Map of split row data.
    void split_row_estimate_data(CellList::SplitRowDataMapT &split_row_data);

    /// Creates and returns a scanner on the immutable cache.
    /// If #m_immutable_cache is not empty, then a scanner is created on it and
    /// returned, otherwise, 0 is returned.
    /// @param scan_ctx Scan context for initializing immutable cache scanner
    /// @return Newly created scanner on the immutable cache, or 0 if an
    /// immutable cache is not installed.
    CellListScanner *create_immutable_scanner(ScanContextPtr &scan_ctx) {
      return m_immutable_cache ? 
        m_immutable_cache->create_scanner(scan_ctx) : nullptr;
    }

    /// Locks the active cache.
    void lock() { m_active_cache->lock(); }

    /// Unlocks the active cache.
    void unlock() { m_active_cache->unlock(); }

    /// Returns a pointer to the active cache.
    /// @return Pointer to the active cache.
    CellCache *active_cache() { return m_active_cache.get(); }

    /// Returns a pointer to the immutable cache.
    /// Returns a pointer to the immutable cache if it is installed, otherewise
    /// nullptr.
    /// @return Pointer to the immutable cache.
    CellCache *immutable_cache() {
      return m_immutable_cache ? m_immutable_cache.get() : nullptr;
    }

    /// Drops the immutable cache.
    void drop_immutable_cache() { m_immutable_cache = nullptr; }

    /// Returns the number of cells in the immutable cache.
    /// @return Number of cells in the immutable cache.
    size_t immutable_items() {
      return m_immutable_cache ? m_immutable_cache->size() : 0;
    }

    /// Checks if active and immutable caches are empty.
    /// @return <i>true<i> if active cache is empty and immutable cache is not
    /// installed or is empty, <i>false</i> otherwise.
    bool empty() {
      return m_active_cache->empty() && immutable_cache_empty();
    }

    /// Checks if immutable cache is not installed or is empty.
    /// @return <i>true<i> if immutable cache is not installed or is empty,
    /// <i>false</i> otherwise.
    bool immutable_cache_empty() {
      return !m_immutable_cache || m_immutable_cache->empty();
    }

    /// Returns the amount of memory used by the active and immutable caches.
    /// This member function calls CellCache::memory_used() on the active and
    /// immutable caches and returns their sum.
    /// @return Amount of memory used by the active and immutable caches.
    int64_t memory_used();

    /// Returns the logical amount of memory used by the active and immutable
    /// caches.  This member function calls CellCache::logical_size() on the
    /// active and immutable caches and returns their sum.
    /// @return Amount of logical memory used by the active and immutable caches
    int64_t logical_size();

    /// Gets cache statistics.
    /// This function computes the sum of the cache statistics from the active
    /// and immutable caches.
    /// @param stats Reference to cell cache statistics structure
    void get_cache_statistics(CellCache::Statistics &stats);

    /// Returns the number of deletes present in the caches.
    /// This function returns the sum of the delete counts from the active and
    /// immutable caches.
    /// @return Number of deletes present in the caches.
    int32_t delete_count();

    /// Freezes the active cache.
    /// This function sets the immutable cache to the active cache and points
    /// the active cache to a newly allocated (empty) cache.
    void freeze();

    /// Populates a set with all keys in the cell caches.
    /// This function inserts all keys from the active cache and the immutable
    /// cache into <code>keys</code>.
    /// @param keys %Key set
    void populate_key_set(KeySet &keys);

  private:

    /// Active cache
    CellCachePtr m_active_cache;

    /// Immutable cache
    CellCachePtr m_immutable_cache;
  };

  /// Smart pointer to CellCacheManager
  typedef intrusive_ptr<CellCacheManager> CellCacheManagerPtr;

  /// @}

} // namespace Hypertable;

#endif // Hypertable_RangeServer_CellCacheManager_h
