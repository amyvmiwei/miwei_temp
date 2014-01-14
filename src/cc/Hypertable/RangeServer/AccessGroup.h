/* -*- c++ -*-
 * Copyright (C) 2007-2013 Hypertable, Inc.
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
/// Declarations for AccessGroup.
/// This file contains declarations for AccessGroup, a class for providing data
/// management and queries over an access group of a range.

#ifndef HYPERTABLE_ACCESSGROUP_H
#define HYPERTABLE_ACCESSGROUP_H

#include <Hypertable/RangeServer/AccessGroupGarbageTracker.h>
#include <Hypertable/RangeServer/CellCacheManager.h>
#include <Hypertable/RangeServer/CellStore.h>
#include <Hypertable/RangeServer/CellStoreInfo.h>
#include <Hypertable/RangeServer/LiveFileTracker.h>
#include <Hypertable/RangeServer/MaintenanceFlag.h>

#include <Hypertable/Lib/Schema.h>
#include <Hypertable/Lib/Types.h>

#include <Common/PageArena.h>
#include <Common/String.h>
#include <Common/StringExt.h>

#include <boost/thread/condition.hpp>

#include <cstdio>
#include <ctime>
#include <fstream>
#include <iostream>
#include <queue>
#include <set>
#include <vector>

namespace Hypertable {

  /// @addtogroup RangeServer
  /// @{

  class AccessGroup : public CellList {

  public:

    class CellStoreMaintenanceData {
    public:
      void *user_data;
      CellStoreMaintenanceData *next;
      CellStore *cs;
      CellStore::IndexMemoryStats index_stats;
      uint64_t shadow_cache_size;
      int64_t  shadow_cache_ecr;
      uint32_t shadow_cache_hits;
      int16_t  maintenance_flags;
    };

    class MaintenanceData {
    public:
      void *user_data;
      MaintenanceData *next;
      AccessGroup *ag;
      CellStoreMaintenanceData *csdata;
      int64_t earliest_cached_revision;
      int64_t latest_stored_revision;
      int64_t mem_used;
      int64_t mem_allocated;
      uint64_t cell_count;
      int64_t disk_used;
      int64_t disk_estimate;
      int64_t log_space_pinned;
      int64_t key_bytes;
      int64_t value_bytes;
      uint32_t file_count;
      int32_t deletes;
      int32_t outstanding_scanners;
      float    compression_ratio;
      int16_t  maintenance_flags;
      uint64_t block_index_memory;
      uint64_t bloom_filter_memory;
      uint32_t bloom_filter_accesses;
      uint32_t bloom_filter_maybes;
      uint32_t bloom_filter_fps;
      uint64_t shadow_cache_memory;
      bool     in_memory;
      bool     gc_needed;
      bool     needs_merging;
      bool     end_merge;
    };

    class Hints {
    public:
      Hints() : latest_stored_revision(TIMESTAMP_MIN), disk_usage(0) { }
      void clear() {
        ag_name.clear();
        latest_stored_revision = TIMESTAMP_MIN;
        disk_usage = 0;
        files.clear();
      }
      bool operator==(const Hints &other) const {
        return ag_name == other.ag_name &&
          latest_stored_revision == other.latest_stored_revision &&
          disk_usage == other.disk_usage &&
          files == other.files;
      }
      String ag_name;
      int64_t latest_stored_revision;
      uint64_t disk_usage;
      String files;
    };

    AccessGroup(const TableIdentifier *identifier, SchemaPtr &schema,
                Schema::AccessGroup *ag, const RangeSpec *range,
                const Hints *hints=0);

    virtual void add(const Key &key, const ByteString value);

    void split_row_estimate_data_cached(SplitRowDataMapT &split_row_data);

    void split_row_estimate_data_stored(SplitRowDataMapT &split_row_data);

    /** Populates <code>scanner</code> with data for <i>.cellstore.index</i>
     * pseudo table.  For each CellStore that is part of the access group,
     * the CellStore::populate_index_pseudo_table_scanner method is called
     * with <code>scanner</code> to gather <i>.cellstore.index</i> pseudo
     * table data.
     * @param scanner Pointer to CellListScannerBuffer to hold data
     */
    void populate_cellstore_index_pseudo_table_scanner(CellListScannerBuffer *scanner);

    void update_schema(SchemaPtr &schema_ptr, Schema::AccessGroup *ag);

    void lock() {
      m_mutex.lock();
      m_cell_cache_manager->lock();
    }

    void unlock() {
      m_cell_cache_manager->unlock();
      m_mutex.unlock();
    }

    CellListScanner *create_scanner(ScanContextPtr &scan_ctx);

    bool include_in_scan(ScanContextPtr &scan_ctx);
    uint64_t disk_usage();
    uint64_t memory_usage();
    void space_usage(int64_t *memp, int64_t *diskp);

    void load_cellstore(CellStorePtr &cellstore);

    void pre_load_cellstores() {
      ScopedLock lock(m_mutex);
      m_latest_stored_revision = TIMESTAMP_MIN;
    }

    void post_load_cellstores() {
      sort_cellstores_by_timestamp();
      get_merge_info(m_needs_merging, m_end_merge);
      if (!m_in_memory &&
          m_latest_stored_revision > m_latest_stored_revision_hint)
        purge_stored_cells_from_cache();
      m_garbage_tracker.update_cellstore_info(m_stores, time(0), true);
    }

    void measure_garbage(double *total, double *garbage);

    void run_compaction(int maintenance_flags, Hints *hints);

    uint64_t purge_memory(MaintenanceFlag::Map &subtask_map);

    MaintenanceData *get_maintenance_data(ByteArena &arena, time_t now,
                                          int flags);

    void stage_compaction();

    void unstage_compaction();

    const char *get_name() { return m_name.c_str(); }

    const char *get_full_name() { return m_full_name.c_str(); }

    void shrink(String &split_row, bool drop_high, Hints *hints);

    void get_file_data(String &file_list, int64_t *block_countp, bool include_blocked) {
      m_file_tracker.get_file_data(file_list, block_countp, include_blocked);
    }

    void release_files(const std::vector<String> &files);

    void recovery_initialize() { m_recovering = true; }
    void recovery_finalize() { m_recovering = false; }

    void dump_keys(std::ofstream &out);

    /// Prints human-readable representation of garbage tracker state to an
    /// output stream.  This function prints the state of the garbage tracker
    /// to <code>out</code> by calling AccessGroupGarbageTracker::output_state()
    /// with the line prefix label being the fully qualified name of the access
    /// group.
    /// @param out Output stream on which to print state
    void dump_garbage_tracker_statistics(std::ofstream &out);

    void set_next_csid(uint32_t nid) {
      if (nid > m_next_cs_id) {
        m_next_cs_id = nid;
        m_file_tracker.set_next_csid(nid);
      }
    }

    void load_hints(Hints *hints);

    String describe();

  private:

    void purge_stored_cells_from_cache();
    void merge_caches();
    void range_dir_initialize();
    void recompute_compression_ratio(int64_t *total_index_entriesp=0);

    bool find_merge_run(size_t *indexp=0, size_t *lenp=0);

    /** Gets merging compaction information.
     * Determines whether or not a merging compaction is needed, and if so,
     * whether or not the "merge run" includes the end cell store (the one
     * containing the most recent data) and therefore the cell cache can be
     * included in the merging compaction.
     * @param needs_merging Set to <i>true</i> if merge is needed
     * @param end_merge Set to <i>true</i> if merge run includes end cell store
     */
    void get_merge_info(bool &needs_merging, bool &end_merge) {
      size_t index, length;
      needs_merging = find_merge_run(&index, &length);
      if (needs_merging)
        end_merge = (index + length) == m_stores.size();
    }

    void sort_cellstores_by_timestamp();

    Mutex m_mutex;
    Mutex m_schema_mutex;
    Mutex m_outstanding_scanner_mutex;
    boost::condition m_outstanding_scanner_cond;
    int32_t m_outstanding_scanner_count {};
    TableIdentifierManaged m_identifier;
    SchemaPtr m_schema;
    std::set<uint8_t> m_column_families;
    String m_name;
    String m_full_name;
    String m_table_name;
    String m_range_dir;
    String m_start_row;
    String m_end_row;
    String m_range_name;
    std::vector<CellStoreInfo> m_stores;
    PropertiesPtr m_cellstore_props;
    CellCacheManagerPtr m_cell_cache_manager;
    uint32_t m_next_cs_id {};
    uint64_t m_disk_usage {};
    float m_compression_ratio {1.0};
    int64_t m_earliest_cached_revision {TIMESTAMP_MAX};
    int64_t m_earliest_cached_revision_saved {TIMESTAMP_MAX};
    int64_t m_latest_stored_revision {TIMESTAMP_MIN};
    int64_t m_latest_stored_revision_hint {TIMESTAMP_MIN};
    LiveFileTracker m_file_tracker;
    AccessGroupGarbageTracker m_garbage_tracker;
    bool m_is_root {};
    bool m_in_memory {};
    bool m_recovering {};
    bool m_bloom_filter_disabled {};
    bool m_needs_merging {};
    bool m_end_merge {};
    bool m_dirty {};
    bool m_cellcache_needs_compaction {};
  };
  typedef boost::intrusive_ptr<AccessGroup> AccessGroupPtr;

  std::ostream &operator<<(std::ostream &os, const AccessGroup::MaintenanceData &mdata);

  /// @}

} // namespace Hypertable

#endif // HYPERTABLE_ACCESSGROUP_H
