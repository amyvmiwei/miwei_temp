/** -*- c++ -*-
 * Copyright (C) 2007-2012 Hypertable, Inc.
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

#ifndef HYPERTABLE_CELLCACHEMANAGER_H
#define HYPERTABLE_CELLCACHEMANAGER_H

#include "CellCache.h"
#include "CellList.h"
#include "CellListScanner.h"
#include "MergeScanner.h"
#include "ScanContext.h"
#include "Common/ReferenceCount.h"
#include "Hypertable/Lib/Schema.h"

namespace Hypertable {

  class CellCacheManager : public ReferenceCount {

  public:
    CellCacheManager();
    virtual ~CellCacheManager() { }

    void install_new_cell_cache(CellCachePtr &cell_cache);

    void install_new_immutable_cache(CellCachePtr &cell_cache);

    void merge_caches(SchemaPtr &schema);

    /**
     * Adds a key/value pair to the write cache.  This method assumes that
     * the write cache has been locked by a call to CellCache#lock.  Copies of
     * the key and value are created and inserted into the underlying cell map
     *
     * @param key key to be inserted
     * @param value value to inserted
     */
    void add(const Key &key, const ByteString value);

    void add_counter(const Key &key, const ByteString value);

    void add_to_read_cache(CellListScannerPtr &scanner);

    void add_immutable_scanner(MergeScanner *scanner, ScanContextPtr &scan_context);

    void add_scanners(MergeScanner *scanner, ScanContextPtr &scan_context);

    void split_row_estimate_data(CellList::SplitRowDataMapT &split_row_data);

    int64_t get_total_entries();

    CellListScanner *create_immutable_scanner(ScanContextPtr &scan_ctx);

    void lock_write_cache();
    void unlock_write_cache();

    void get_read_cache(CellCachePtr &read_cache);

    CellCache *immutable_cache() { return m_immutable_cache ? m_immutable_cache.get() : 0; }

    void drop_immutable_cache() { m_immutable_cache = 0; }

    size_t immutable_items();

    bool empty();

    bool immutable_cache_empty();

    /** Returns the amount of memory used by the CellCache.  This is the
     * summation of the lengths of all the keys and values in the map.
     */
    int64_t memory_used();

    int64_t logical_size();

    /**
     * Returns the amount of memory allocated by the CellCache.
     */
    uint64_t memory_allocated();

    void get_counts(size_t *cellsp, int64_t *key_bytesp, int64_t *value_bytesp);

    int32_t mutable_delete_count();

    int32_t delete_count();

    void freeze();

    void populate_key_set(KeySet &keys);

  private:
    CellCachePtr m_read_cache;
    CellCachePtr m_write_cache;
    CellCachePtr m_immutable_cache;
  };

  typedef intrusive_ptr<CellCacheManager> CellCacheManagerPtr;

} // namespace Hypertable;

#endif // HYPERTABLE_CELLCACHEMANAGER_H
