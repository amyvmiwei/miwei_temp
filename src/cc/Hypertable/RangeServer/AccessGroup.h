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

#ifndef HYPERTABLE_ACCESSGROUP_H
#define HYPERTABLE_ACCESSGROUP_H

#include <queue>
#include <set>
#include <vector>
#include <cstdio>
#include <ctime>
#include <iostream>
#include <fstream>

#include <boost/thread/condition.hpp>

#include "Common/PageArena.h"
#include "Common/String.h"
#include "Common/StringExt.h"
#include "Common/HashMap.h"

#include "Hypertable/Lib/Schema.h"
#include "Hypertable/Lib/Types.h"

#include "AccessGroupGarbageTracker.h"
#include "CellCacheManager.h"
#include "CellStore.h"
#include "CellStoreTrailerV6.h"
#include "CellStoreInfo.h"
#include "LiveFileTracker.h"
#include "MaintenanceFlag.h"


namespace Hypertable {

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
    };

    AccessGroup(const TableIdentifier *identifier, SchemaPtr &schema,
                Schema::AccessGroup *ag, const RangeSpec *range);

    virtual void add(const Key &key, const ByteString value);

    void split_row_estimate_data_cached(SplitRowDataMapT &split_row_data);

    void split_row_estimate_data_stored(SplitRowDataMapT &split_row_data);

    virtual int64_t get_total_entries() {
      boost::mutex::scoped_lock lock(m_mutex);
      int64_t total = m_cell_cache_manager->get_total_entries();
      if (!m_in_memory) {
        for (size_t i=0; i<m_stores.size(); i++)
          total += m_stores[i].cs->get_total_entries();
      }
      return total;
    }

    void update_schema(SchemaPtr &schema_ptr, Schema::AccessGroup *ag);

    void lock() {
      m_mutex.lock();
      m_cell_cache_manager->lock_write_cache();
    }

    void unlock() {
      m_cell_cache_manager->unlock_write_cache();
      m_mutex.unlock();
    }

    CellListScanner *create_scanner(ScanContextPtr &scan_ctx);

    bool include_in_scan(ScanContextPtr &scan_ctx);
    uint64_t disk_usage();
    uint64_t memory_usage();
    void space_usage(int64_t *memp, int64_t *diskp);
    void add_cell_store(CellStorePtr &cellstore);

    void compute_garbage_stats(uint64_t *input_bytesp, uint64_t *output_bytesp);

    void run_compaction(int maintenance_flags);

    uint64_t purge_memory(MaintenanceFlag::Map &subtask_map);

    MaintenanceData *get_maintenance_data(ByteArena &arena, time_t now);

    void stage_compaction();

    void unstage_compaction();

    const char *get_name() { return m_name.c_str(); }

    const char *get_full_name() { return m_full_name.c_str(); }

    void shrink(String &split_row, bool drop_high);

    void get_file_data(String &file_list, int64_t *block_countp, bool include_blocked) {
      m_file_tracker.get_file_data(file_list, block_countp, include_blocked);
    }

    void release_files(const std::vector<String> &files);

    void recovery_initialize() { m_recovering = true; }
    void recovery_finalize() {
      sort_cellstores_by_timestamp();
      m_needs_merging = find_merge_run();
      m_recovering = false;
    }

    void dump_keys(std::ofstream &out);

    void set_next_csid(uint32_t nid) {
      if (nid > m_next_cs_id) {
        m_next_cs_id = nid;
        m_file_tracker.set_next_csid(nid);
      }
    }

  private:

    void merge_caches(bool reset_earliest_cached_revision=true);
    void range_dir_initialize();
    void recompute_compression_ratio(int64_t *total_index_entriesp=0);
    bool find_merge_run(size_t *indexp=0, size_t *lenp=0);
    bool needs_merging();
    void sort_cellstores_by_timestamp();

    Mutex                m_mutex;
    Mutex                m_outstanding_scanner_mutex;
    boost::condition     m_outstanding_scanner_cond;
    int32_t              m_outstanding_scanner_count;
    TableIdentifierManaged m_identifier;
    SchemaPtr            m_schema;
    std::set<uint8_t>    m_column_families;
    String               m_name;
    String               m_full_name;
    String               m_table_name;
    String               m_range_dir;
    String               m_start_row;
    String               m_end_row;
    String               m_range_name;
    std::vector<CellStoreInfo> m_stores;
    PropertiesPtr        m_cellstore_props;
    CellCacheManagerPtr  m_cell_cache_manager;
    uint32_t             m_next_cs_id;
    uint64_t             m_disk_usage;
    float                m_compression_ratio;
    int64_t              m_compaction_revision;
    int64_t              m_earliest_cached_revision;
    int64_t              m_earliest_cached_revision_saved;
    int64_t              m_latest_stored_revision;
    uint64_t             m_collisions;
    LiveFileTracker      m_file_tracker;
    AccessGroupGarbageTracker m_garbage_tracker;
    bool                 m_is_root;
    bool                 m_in_memory;
    bool                 m_recovering;
    bool                 m_bloom_filter_disabled;
    bool                 m_needs_merging;

  };
  typedef boost::intrusive_ptr<AccessGroup> AccessGroupPtr;

  std::ostream &operator<<(std::ostream &os, const AccessGroup::MaintenanceData &mdata);

} // namespace Hypertable

#endif // HYPERTABLE_ACCESSGROUP_H
